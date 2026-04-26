// TODO
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
    Api, Client, ingress,
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
            std::env::set_var("RUST_LOG", "warn,pangolin_ingress_controller=info");
        }
    }

    let client = Client::try_default().await?;
    let config = Config::default().debounce(Duration::from_secs(5));
    let svc = Api::<Service>::all(client.clone());
    BFallController::new(client.clone(), config, ingress)
        .await
        .run::<Reconciler, ErrorPolicy, Data>(Data {
            client,
            leader_status: None,
        })
        .await?;

    Ok(())
}
