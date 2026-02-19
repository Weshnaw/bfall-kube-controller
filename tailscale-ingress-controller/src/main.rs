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
    api::{DeleteParams, ListParams, ObjectMeta, Patch, PatchParams},
    runtime::{
        Controller,
        controller::{Action, Config},
        reflector::Lookup,
        watcher,
    },
};
use kube_leader_election::{LeaseLock, LeaseLockParams};
use metrics::{counter, gauge, histogram};
// use metrics_dashboard::{ChartType, DashboardOptions, build_dashboard_route};
// use poem::{EndpointExt, Route, Server, listener::TcpListener, middleware::Tracing};
use tokio::{sync::Notify, time::Instant};
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

struct Data {
    client: Client,
    leader_status: Arc<AtomicBool>,
}

#[derive(Debug, Display, Error, From)]
enum AppError {
    KubeError(kube::Error),
    LeaderElectionError(kube_leader_election::Error),
    #[from(skip)]
    MissingObjectKey(#[error(not(source))] &'static str),
    #[from(skip)]
    CouldNotCreateResource(#[error(not(source))] &'static str),
}

#[tracing::instrument(level = "debug", skip(ctx, svc), fields(svc.name = svc.metadata.name, svc.namespace=svc.metadata.namespace))]
async fn reconcile(svc: Arc<Service>, ctx: Arc<Data>) -> Result<Action, AppError> {
    if !ctx.leader_status.load(Ordering::Relaxed) {
        debug!("Skipping reconciliation as not leader");
        return Ok(Action::requeue(Duration::from_mins(5)));
    }
    let start = Instant::now();
    counter!("reconciled").increment(1);
    let client = &ctx.client;
    let namespace = svc
        .metadata
        .namespace
        .as_ref()
        .ok_or(AppError::MissingObjectKey("metadata.namespace"))?;
    let name = svc
        .metadata
        .name
        .as_ref()
        .ok_or(AppError::MissingObjectKey("metadata.name"))?;
    let service_uid = svc
        .metadata
        .uid
        .as_ref()
        .ok_or(AppError::MissingObjectKey("metadata.uid"))?;
    let mut ingress_name = format!("tsi-{name}");
    let ingress_api = Api::<Ingress>::namespaced(client.clone(), namespace);
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

        let existing = ingress_api
            .list(&ListParams::default().labels(&format!(
                "bfall.me/parent-uid={},bfall.me/managed=true",
                service_uid
            )))
            .await?;

        let (ingress_name, namespace) = if let Some(ingress) = existing.items.first() {
            debug!("Ingress {} already exists, updating", ingress_name);
            (
                ingress
                    .metadata
                    .name
                    .clone()
                    .ok_or(AppError::MissingObjectKey("metadata.name"))?,
                ingress
                    .metadata
                    .namespace
                    .clone()
                    .ok_or(AppError::MissingObjectKey("metadata.name"))?,
            )
        } else {
            debug!("Ingress {} does not exist, creating", ingress_name);

            let mut found_usable_name = false;
            for i in 0..20 {
                let existing = ingress_api
                    .list(&ListParams::default().fields(&format!("metadata.name={}", ingress_name)))
                    .await?;

                if existing.items.is_empty() {
                    found_usable_name = true;
                    break;
                }

                ingress_name = format!("tsi-{name}-{i:02}");
            }

            if !found_usable_name {
                warn!("Failed to find a usable name for the ingress");
                return Err(AppError::CouldNotCreateResource(
                    "Failed to find a usable name for the ingress",
                ));
            }
            (ingress_name, namespace.clone())
        };

        let annotations = if should_funnel {
            let mut map = BTreeMap::new();
            map.insert(String::from("tailscale.com/funnel"), String::from("true"));
            Some(map)
        } else {
            None
        };

        let mut labels = BTreeMap::new();
        labels.insert(String::from("bfall.me/managed"), String::from("true"));
        labels.insert(String::from("bfall.me/parent-uid"), service_uid.clone());
        labels.insert(
            String::from("bfall.me/version"),
            built_info::PKG_VERSION.into(),
        );
        if let Some(hash) = built_info::GIT_COMMIT_HASH {
            labels.insert(String::from("bfall.me/commit"), hash.into());
        }

        let ingress = Ingress {
            metadata: ObjectMeta {
                name: Some(ingress_name.clone()),
                namespace: Some(namespace.clone()),
                owner_references: Some(vec![owner_ref]),
                annotations,
                labels: Some(labels),
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

        counter!("reconciled_patch").increment(1);
        // Use patch with apply to create or update the ingress
        ingress_api
            .patch(
                &ingress_name,
                &PatchParams::apply(built_info::PKG_NAME),
                &Patch::Apply(&ingress),
            )
            .await?;
        histogram!("reconcile_patch_time").record(start.elapsed().as_secs_f32());
    } else {
        let existing = ingress_api
            .list(&ListParams::default().labels(&format!(
                "bfall.me/parent-uid={},bfall.me/managed=true",
                service_uid
            )))
            .await?;
        debug!("No labels found for svc");

        for ingress in &existing {
            if let Some(ingress) = ingress.name() {
                counter!("reconciled_remove").increment(1);
                info!("Removing ingress: {}", &ingress);
                ingress_api
                    .delete(&ingress, &DeleteParams::default())
                    .await?;
            }
        }
        if !existing.items.is_empty() {
            histogram!("reconcile_delete_time").record(start.elapsed().as_secs_f32());
        }
    }
    histogram!("reconcile_time").record(start.elapsed().as_secs_f32());
    Ok(Action::requeue(Duration::from_mins(5)))
}

/// The controller triggers this on reconcile errors
#[tracing::instrument(level = "warn", skip(_ctx, _svc))]
fn error_policy(_svc: Arc<Service>, e: &AppError, _ctx: Arc<Data>) -> Action {
    warn!("Reconcile error: {}", e);
    counter!("reconciled_error").increment(1);
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
    let leader_guage = gauge!("leader_status");
    leader_guage.set(0);
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
    tokio::spawn(async move {
        let leadership = tr_leadership;
        let status = tr_status;
        let notify = tr_notify;
        info!("Starting leader election...");
        loop {
            match leadership.try_acquire_or_renew().await {
                Ok(_) => {
                    status.store(true, Ordering::SeqCst);
                    notify.notify_one();
                    leader_guage.set(1);
                    debug!("Leader election succeeded...")
                }
                Err(e) => {
                    leader_guage.set(0);
                    status.store(false, Ordering::SeqCst);
                    warn!("failed to acquire or renew leadership: {}", e);
                }
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    // tokio::spawn(async move {
    //     let port = std::env::var("PORT").unwrap_or("3000".into());
    //     let server = format!("0.0.0.0:{}", port);

    //     info!("Metrics listening on {}", server);

    //     let dashboard_options = DashboardOptions {
    //         custom_charts: vec![],
    //         include_default: true,
    //     };

    //     let app = Route::new()
    //         .nest("/", build_dashboard_route(dashboard_options))
    //         .with(Tracing);

    //     Server::new(TcpListener::bind(server))
    //         .name("metrics")
    //         .run(app)
    //         .await
    //         .unwrap();
    // });

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
                Ok(o) => debug!("reconciled {:?}", o),
                Err(e) => warn!("reconcile failed: {}", e),
            }
        })
        .await;

    info!("Controller terminated...");
    leadership.step_down().await?;

    Ok(())
}
