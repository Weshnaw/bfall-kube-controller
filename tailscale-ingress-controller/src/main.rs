use std::{
    collections::BTreeMap,
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
use kube_leader_election::{LeaseLock, LeaseLockParams, LeaseLockResult};
use metrics::{counter, gauge, histogram};
// use metrics_dashboard::{ChartType, DashboardOptions, build_dashboard_route};
// use poem::{EndpointExt, Route, Server, listener::TcpListener, middleware::Tracing};
use tokio::{
    sync::{Mutex, mpsc},
    time::Instant,
};
use tracing::{debug, info, trace, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use uuid::Uuid;

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
        return Ok(Action::requeue(Duration::from_secs(30)));
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

        let owner_ref = svc
            .controller_owner_ref(&())
            .ok_or(AppError::MissingObjectKey("metadata.owner_references"))?;

        let existing = ingress_api
            .list(&ListParams::default().labels(&format!(
                "bfall.me/parent-uid={},bfall.me/managed=true",
                service_uid
            )))
            .await?;

        let (ingress_name, namespace) = if let Some(ingress) = existing.items.first() {
            info!("Patching ingress: port:{port} should_funnel:{should_funnel}");
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
            info!("Creating tailscale ingress: port:{port} should_funnel:{should_funnel}");

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
    if std::env::var("RUST_LOG").is_err() {
        // We are just setting a default RUST_LOG value
        unsafe {
            std::env::set_var("RUST_LOG", "warn,tailscale_ingress_controller=info");
        }
    }
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    info!("Initializing client...");

    let client = Client::try_default().await?;

    let lease_namespace = std::env::var("NAMESPACE").unwrap_or("default".into());

    let (leader_tx, leader_rx) = mpsc::channel(10);
    let leader_status = Arc::new(AtomicBool::new(false));
    let leader_guage = gauge!("leader_status");
    leader_guage.set(0);

    let uuid = Uuid::new_v4().to_string();
    let leadership = Arc::new(LeaseLock::new(
        client.clone(),
        &lease_namespace,
        LeaseLockParams {
            holder_id: uuid.clone(),
            lease_name: "tailscale-ingress-controller-lock".into(),
            lease_ttl: Duration::from_secs(15),
        },
    ));
    let leadership_clone = leadership.clone();
    let status_clone = leader_status.clone();
    tokio::spawn(async move {
        let leadership = leadership_clone;
        let status = status_clone;
        info!("Starting leader election uuid={uuid}...");
        loop {
            match leadership.try_acquire_or_renew().await {
                Ok(LeaseLockResult::Acquired(_)) => {
                    debug!("Lease acquired...");
                    leader_guage.set(1);
                    status.store(true, Ordering::SeqCst);
                    leader_tx.try_send(true).ok();
                }
                Ok(lease) => {
                    debug!("Unable to acquire lease...\n{:?}", lease);
                    leader_guage.set(0);
                    status.store(false, Ordering::SeqCst);
                    leader_tx.try_send(false).ok();
                }
                Err(e) => {
                    warn!("failed to acquire or renew leadership: {}", e);
                    leader_guage.set(0);
                    status.store(false, Ordering::SeqCst);
                    leader_tx.try_send(false).ok();
                }
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    let should_end_loop = Arc::new(AtomicBool::new(true));
    let leader_rx = Arc::new(Mutex::new(leader_rx));
    loop {
        let client = client.clone();
        let leader_status = leader_status.clone();
        let config = Config::default().debounce(Duration::from_secs(5));
        let svc = Api::<Service>::all(client.clone());
        let ingress = Api::<Ingress>::all(client.clone());

        while !leader_rx.lock().await.recv().await.unwrap_or(false) {
            trace!("Waiting for status notification...");
        }

        info!("Starting controller...");
        let should_end_loop_clone = should_end_loop.clone();
        let leader_rx_clone = leader_rx.clone();
        Controller::new(svc, watcher::Config::default())
            .owns(ingress, watcher::Config::default())
            .with_config(config)
            .shutdown_on_signal()
            .graceful_shutdown_on(async move {
                while leader_rx_clone.lock().await.recv().await.unwrap_or(false) {}
                debug!("Leadership lost, restarting controller...");
                should_end_loop_clone.clone().store(false, Ordering::SeqCst);
            })
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
                    Ok(o) => trace!("reconciled {:?}", o),
                    Err(e) => trace!("reconcile failed: {}", e),
                }
            })
            .await;

        if should_end_loop.load(Ordering::SeqCst) {
            break;
        }
        should_end_loop.clone().store(true, Ordering::SeqCst);
    }
    info!("Controller terminated...");
    leadership.step_down().await?;

    Ok(())
}
