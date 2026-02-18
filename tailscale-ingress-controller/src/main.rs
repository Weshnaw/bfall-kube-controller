use std::{
    collections::BTreeMap,
    io::BufRead,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use derive_more::{Debug, Display, Error, From};
use futures::StreamExt;
use k8s_openapi::api::{
    core::v1::Service,
    networking::v1::{
        Ingress, IngressBackend, IngressServiceBackend, IngressSpec, IngressTLS, ServiceBackendPort,
    },
};
use kube::{
    Api, Client, Resource,
    api::{ObjectMeta, Patch, PatchParams},
    runtime::{
        Controller,
        controller::{Action, Config},
        watcher,
    },
};
use kube_leader_election::{LeaseLock, LeaseLockParams};
use tokio::sync::Notify;
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

struct Data {
    client: Client,
    leader_status: Arc<AtomicBool>,
}

#[derive(Debug, Display, Error, From)]
enum AppError {
    KubeError(kube::Error),
    LeaderElectionError(kube_leader_election::Error),
    MissingObjectKey(#[error(not(source))] &'static str),
}

#[tracing::instrument(level = "debug", skip(ctx, svc), fields(svc.name = svc.metadata.name, svc.namespace=svc.metadata.namespace))]
async fn reconcile(svc: Arc<Service>, ctx: Arc<Data>) -> Result<Action, AppError> {
    if ctx.leader_status.load(Ordering::Relaxed) {
        info!("Skipping reconciliation as not leader");
        return Ok(Action::requeue(Duration::from_mins(5)));
    }

    let client = &ctx.client;

    if let Some(labels) = &svc.metadata.labels
        && let Some(port) = labels.get("bfall.me/tailscale-ingress")
    {
        let should_funnel = labels
            .get("bfall.me/tailscale-funnel")
            .is_some_and(|val| val.parse::<bool>().unwrap_or(false));
        info!("Creating tailscale ingress: port:{port} should_funnel:{should_funnel}");

        let owner_ref = svc
            .controller_owner_ref(&())
            .ok_or(AppError::MissingObjectKey("metadata.owner_references"))?;

        let name = svc
            .metadata
            .name
            .as_ref()
            .ok_or(AppError::MissingObjectKey("metadata.name"))?;
        let namespace = svc
            .metadata
            .namespace
            .as_ref()
            .ok_or(AppError::MissingObjectKey("metadata.namespace"))?;

        let port = if let Ok(port) = port.parse::<i32>() {
            ServiceBackendPort {
                number: Some(port),
                ..Default::default()
            }
        } else {
            ServiceBackendPort {
                name: Some(port.clone()),
                ..Default::default()
            }
        };

        let annotations = if should_funnel {
            let mut map = BTreeMap::new();
            map.insert(String::from("tailscale.com/funnel"), String::from("true"));
            Some(map)
        } else {
            None
        };

        let ingress = Ingress {
            metadata: ObjectMeta {
                name: Some(format!("tsi-{name}")),
                namespace: Some(namespace.clone()),
                owner_references: Some(vec![owner_ref]),
                annotations,
                ..Default::default()
            },
            spec: Some(IngressSpec {
                default_backend: Some(IngressBackend {
                    service: Some(IngressServiceBackend {
                        name: name.clone(),
                        port: Some(port),
                    }),
                    ..Default::default()
                }),
                ingress_class_name: Some("tailscale".into()),
                tls: Some(vec![IngressTLS {
                    hosts: Some(vec![name.clone()]),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let ingress_api = Api::<Ingress>::namespaced(client.clone(), namespace);

        ingress_api
            .patch(
                ingress
                    .metadata
                    .name
                    .as_ref()
                    .ok_or(AppError::MissingObjectKey("metadata.name"))?,
                &PatchParams::apply("bfall.me/ingress-controller"),
                &Patch::Apply(&ingress),
            )
            .await?;
    } else {
        debug!("No labels found for svc");
    }
    Ok(Action::requeue(Duration::from_mins(5)))
}

/// The controller triggers this on reconcile errors
#[tracing::instrument(level = "warn", skip(_ctx, _svc))]
fn error_policy(_svc: Arc<Service>, e: &AppError, _ctx: Arc<Data>) -> Action {
    warn!("Reconcile error: {}", e);
    Action::requeue(Duration::from_secs(1))
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    info!("Attempting to get leadership...");

    info!("Initializing client...");

    let client = Client::try_default().await?;
    info!("currently leading");

    let config = Config::default();

    info!("Starting ingress generator...");
    info!("press <enter> to force a reconciliation of all objects");

    let (mut reload_tx, reload_rx) = futures::channel::mpsc::channel(0);
    std::thread::spawn(move || {
        for _ in std::io::BufReader::new(std::io::stdin()).lines() {
            let _ = reload_tx.try_send(());
        }
    });

    let svc = Api::<Service>::all(client.clone());
    let ingress = Api::<Ingress>::all(client.clone());

    let lease_namespace = std::env::var("NAMESPACE").unwrap_or("default".into());

    let notify = Arc::new(Notify::new());
    let leader_status = Arc::new(AtomicBool::new(false));
    let leadership = Arc::new(LeaseLock::new(
        client.clone(),
        &lease_namespace,
        LeaseLockParams {
            holder_id: "tailscale-ingress-controller".into(),
            lease_name: "tailscale-ingress-controller-lock".into(),
            lease_ttl: Duration::from_secs(15),
        },
    ));
    let tr_leadership = leadership.clone();
    let tr_status = leader_status.clone();
    let tr_notify = Arc::clone(&notify);
    let leader_election_thread = tokio::spawn(async move {
        let leadership = tr_leadership;
        let status = tr_status;
        let notify = tr_notify;
        info!("Starting leader election...");
        loop {
            match leadership.try_acquire_or_renew().await {
                Ok(_) => {
                    status.store(true, Ordering::SeqCst);
                    notify.notify_one();
                    debug!("Leader election succeeded...")
                }
                Err(e) => {
                    status.store(false, Ordering::SeqCst);
                    warn!("failed to acquire or renew leadership: {}", e);
                }
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    let controller_thread = tokio::spawn(async move {
        while leader_status.load(Ordering::Relaxed) {
            debug!("Waiting for status notification...");
            notify.notified().await;
        }
        info!("Starting controller...");
        Controller::new(svc, watcher::Config::default())
            .owns(ingress, watcher::Config::default())
            .with_config(config)
            .reconcile_all_on(reload_rx.map(|_| ()))
            .shutdown_on_signal()
            .run(
                reconcile,
                error_policy,
                Arc::new(Data {
                    client,
                    leader_status,
                }),
            )
            .for_each(|res| async move {
                match res {
                    Ok(o) => info!("reconciled {:?}", o),
                    Err(e) => warn!("reconcile failed: {}", e),
                }
            })
            .await;
    });

    let _ = tokio::select!( _ = leader_election_thread => {}, _ =controller_thread => {});

    info!("Controller terminated...");
    leadership.step_down().await?;

    Ok(())
}
