use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use gateway_api::gateways::Gateway;
use kube::{
    Api, Client,
    runtime::controller::{Action, Config},
};
use metrics::{counter, histogram};
use shared::controller::{BFallController, CheckLeadershipStatus};
use tokio::time::Instant;
use tracing::{debug, warn};

#[tracing::instrument(level = "debug", skip(ctx, svc), fields(svc.name = svc.metadata.name, svc.namespace=svc.metadata.namespace))]
async fn reconcile(svc: Arc<Gateway>, ctx: Arc<Data>) -> Result<Action, shared::Error> {
    if !ctx.is_leader() {
        debug!("Skipping reconciliation as not leader");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }
    let start = Instant::now();
    counter!("reconciled").increment(1);
    let _client = &ctx.client;
    let _namespace = svc
        .metadata
        .namespace
        .as_ref()
        .ok_or(shared::Error::MissingObjectKey("metadata.namespace"))?;
    let _name = svc
        .metadata
        .name
        .as_ref()
        .ok_or(shared::Error::MissingObjectKey("metadata.name"))?;
    let _service_uid = svc
        .metadata
        .uid
        .as_ref()
        .ok_or(shared::Error::MissingObjectKey("metadata.uid"))?;
    histogram!("reconcile_time").record(start.elapsed().as_secs_f32());
    Ok(Action::requeue(Duration::from_mins(5)))
}

/// The controller triggers this on reconcile errors
#[tracing::instrument(level = "warn", skip(_ctx, _svc))]
fn error_policy(_svc: Arc<Gateway>, e: &shared::Error, _ctx: Arc<Data>) -> Action {
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

impl shared::controller::Reconciler<Gateway, Data> for Reconciler {
    type ReconcilerFut =
        Pin<Box<dyn Future<Output = Result<Action, shared::Error>> + Send + 'static>>;
    fn reconcile(key: Arc<Gateway>, context: Arc<Data>) -> Self::ReconcilerFut {
        Box::pin(reconcile(key, context))
    }
}

struct ErrorPolicy;

impl shared::controller::ErrorPolicy<Gateway, shared::Error, Data> for ErrorPolicy {
    fn error_policy(key: Arc<Gateway>, error: &shared::Error, context: Arc<Data>) -> Action {
        error_policy(key, error, context)
    }
}
pub async fn controller(client: Client, config: Config) -> Result<(), shared::Error> {
    let gateway = Api::<Gateway>::all(client.clone());
    BFallController::new(client.clone(), config, gateway)
        .await
        .run::<Reconciler, ErrorPolicy, Data>(Data {
            client,
            leader_status: None,
        })
        .await?;

    Ok(())
}
