use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

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
        controller::{Action, Config},
        reflector::Lookup,
    },
};
use metrics::{counter, histogram};
use shared::controller::{BFallController, CheckLeadershipStatus};
use tokio::time::Instant;
use tracing::{debug, info, warn};

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[tracing::instrument(level = "debug", skip(ctx, svc), fields(svc.name = svc.metadata.name, svc.namespace=svc.metadata.namespace))]
async fn reconcile(svc: Arc<Service>, ctx: Arc<Data>) -> Result<Action, shared::Error> {
    if !ctx.is_leader() {
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
        .ok_or(shared::Error::MissingObjectKey("metadata.namespace"))?;
    let name = svc
        .metadata
        .name
        .as_ref()
        .ok_or(shared::Error::MissingObjectKey("metadata.name"))?;
    let service_uid = svc
        .metadata
        .uid
        .as_ref()
        .ok_or(shared::Error::MissingObjectKey("metadata.uid"))?;
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
            .ok_or(shared::Error::MissingObjectKey("metadata.owner_references"))?;

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
                    .ok_or(shared::Error::MissingObjectKey("metadata.name"))?,
                ingress
                    .metadata
                    .namespace
                    .clone()
                    .ok_or(shared::Error::MissingObjectKey("metadata.name"))?,
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
                return Err(shared::Error::CouldNotCreateResource(
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
fn error_policy(_svc: Arc<Service>, e: &shared::Error, _ctx: Arc<Data>) -> Action {
    warn!("Reconcile error: {}", e);
    counter!("reconciled_error").increment(1);
    Action::requeue(Duration::from_secs(1))
}

struct Data {
    client: Client,
    leader_status: Option<Arc<AtomicBool>>,
}

impl CheckLeadershipStatus for Data {
    fn is_leader(&self) -> bool {
        self.leader_status
            .as_ref()
            .is_some_and(|status| status.load(Ordering::Relaxed))
    }

    fn set_leader_atomic_bool(&mut self, status: Arc<AtomicBool>) {
        self.leader_status = Some(status);
    }
}

struct Reconciler;

impl shared::controller::Reconciler<Service, Data> for Reconciler {
    type ReconcilerFut =
        Pin<Box<dyn Future<Output = Result<Action, shared::Error>> + Send + 'static>>;
    fn reconcile(key: Arc<Service>, context: Arc<Data>) -> Self::ReconcilerFut {
        Box::pin(reconcile(key, context))
    }
}

struct ErrorPolicy;

impl shared::controller::ErrorPolicy<Service, shared::Error, Data> for ErrorPolicy {
    fn error_policy(key: Arc<Service>, error: &shared::Error, context: Arc<Data>) -> Action {
        error_policy(key, error, context)
    }
}
#[tokio::main]
async fn main() -> Result<(), shared::Error> {
    if std::env::var("RUST_LOG").is_err() {
        // We are just setting a default RUST_LOG value race conditions don't really matter here
        unsafe {
            std::env::set_var("RUST_LOG", "warn,tailscale_ingress_controller=info");
        }
    }

    let client = Client::try_default().await?;
    let config = Config::default().debounce(Duration::from_secs(5));
    let svc = Api::<Service>::all(client.clone());
    let ingress = Api::<Ingress>::all(client.clone());
    BFallController::new(client.clone(), config, svc)
        .await
        .owns(ingress)
        .run::<Reconciler, ErrorPolicy, Data>(Data {
            client,
            leader_status: None,
        })
        .await?;

    Ok(())
}
