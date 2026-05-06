use std::{pin::Pin, sync::Arc, time::Duration};

use gateway_api::apis::experimental::{
    gatewayclasses::GatewayClass, gateways::Gateway, httproutes::HTTPRoute,
};
use kube::{
    Api, Client,
    runtime::{
        controller::{Action, Config},
        reflector::Store,
    },
};
use metrics::{counter, histogram};
use shared::controller::{BFallController, CheckLeadershipStatus};
use tokio::{sync::watch, time::Instant};
use tracing::{debug, info, warn};

use crate::http_route::{
    fetch::fetch_kubernetes_data,
    sync::{update_kube_statuses, update_pangolin_api},
    validate::validate_against_pangolin_api,
};

mod fetch;
mod sync;
mod validate;

#[tracing::instrument(level = "debug", skip(ctx, hr), fields(hr.name = hr.metadata.name, hr.namespace=hr.metadata.namespace))]
async fn reconcile(hr: Arc<HTTPRoute>, ctx: Arc<Data>) -> Result<Action, shared::Error> {
    let start = Instant::now();
    counter!("hr_reconciled").increment(1);

    let cfg = fetch_kubernetes_data(hr, ctx.clone()).await?;

    validate_against_pangolin_api(&cfg)?;

    // Do API Updates
    if !ctx.is_leader() {
        debug!("Skipping reconciliation as not leader");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }
    update_pangolin_api(&cfg)?;
    update_kube_statuses()?;

    histogram!("hr_reconcile_time").record(start.elapsed().as_secs_f32());
    Ok(Action::requeue(Duration::from_mins(5)))
}

/// The controller triggers this on reconcile errors
#[tracing::instrument(level = "warn", skip(_ctx, _svc))]
fn error_policy(_svc: Arc<HTTPRoute>, e: &shared::Error, _ctx: Arc<Data>) -> Action {
    warn!("Reconcile error: {}", e);
    counter!("hr_reconciled_error").increment(1);
    // TODO: apply error status
    Action::requeue(Duration::from_secs(1))
}

struct Data {
    client: Client,
    gw_store: Store<Gateway>,
    gc_store: Store<GatewayClass>,
    leader_status: Option<watch::Receiver<bool>>,
}

impl CheckLeadershipStatus for Data {
    fn is_leader(&self) -> bool {
        self.leader_status
            .as_ref()
            .is_some_and(|status| *status.borrow())
    }

    fn set_leader(&mut self, status: watch::Receiver<bool>) {
        self.leader_status = Some(status);
    }
}

struct Reconciler;

impl shared::controller::Reconciler<HTTPRoute, Data> for Reconciler {
    type ReconcilerFut =
        Pin<Box<dyn Future<Output = Result<Action, shared::Error>> + Send + 'static>>;
    fn reconcile(key: Arc<HTTPRoute>, context: Arc<Data>) -> Self::ReconcilerFut {
        Box::pin(reconcile(key, context))
    }
}

struct ErrorPolicy;

impl shared::controller::ErrorPolicy<HTTPRoute, shared::Error, Data> for ErrorPolicy {
    fn error_policy(key: Arc<HTTPRoute>, error: &shared::Error, context: Arc<Data>) -> Action {
        error_policy(key, error, context)
    }
}

pub async fn controller(
    client: Client,
    config: Config,
    gw_store: Store<Gateway>,
    gc_store: Store<GatewayClass>,
    lease_details: watch::Receiver<bool>,
) -> Result<(), shared::Error> {
    info!("Initializing 'HTTPRoute' Controller...");

    let route = Api::<HTTPRoute>::all(client.clone());
    BFallController::with_shared_lease(config, route, lease_details)
        .run::<Reconciler, ErrorPolicy, Data>(Data {
            client,
            gw_store,
            gc_store,
            leader_status: None,
        })
        .await
}
