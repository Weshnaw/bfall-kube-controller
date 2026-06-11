use std::{pin::Pin, sync::Arc, time::Duration};

use crate::{CONTROLLER_NAME, pangolin::PangolinApiConfig};
use futures::TryFutureExt;
use gateway_api::apis::experimental::{
    constants::{GatewayClassConditionReason, GatewayClassConditionType},
    gatewayclasses::GatewayClass,
};
use k8s_openapi::{
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
    jiff::Timestamp,
};
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{
        controller::{Action, Config},
        reflector::Store,
    },
};
use metrics::{counter, histogram};
use serde_json::json;
use shared::controller::{BFallController, CheckLeadershipStatus};
use tokio::{sync::watch, time::Instant};
use tracing::{debug, info, warn};

#[tracing::instrument(level = "debug", skip(ctx, gc), fields(gc.name = gc.metadata.name, gc.namespace=gc.metadata.namespace))]
async fn reconcile(gc: Arc<GatewayClass>, ctx: Arc<Data>) -> Result<Action, shared::Error> {
    let start = Instant::now();

    if gc.spec.controller_name != CONTROLLER_NAME {
        return Ok(Action::await_change());
    }

    counter!("gc_reconciled").increment(1);
    let name = gc.name_any();
    info!(name, "Reconciling GatewayClass");

    let client = &ctx.client;

    let api_config = PangolinApiConfig::from_gatewayclass(client, &gc)
        .and_then(|config| config.check_endpoint())
        .await;

    match api_config {
        Ok(_) => {
            let condition = Condition {
                last_transition_time: Time(Timestamp::now()),
                message: "GatewayClass is accepted by the gateway-controller".into(),
                observed_generation: gc.metadata.generation,
                reason: GatewayClassConditionReason::Accepted.to_string(),
                status: "True".into(),
                type_: GatewayClassConditionType::Accepted.to_string(),
            };

            // TODO: graceful shutdown update the conditions to false
            let status_patch = json!({
                "status": {
                    "conditions": [condition],
                }
            });
            let api: Api<GatewayClass> = Api::all(client.clone());

            if !ctx.is_leader() {
                debug!("Skipping reconciliation as not leader");
                return Ok(Action::requeue(Duration::from_secs(30)));
            }

            api.patch_status(&name, &PatchParams::default(), &Patch::Merge(status_patch))
                .await?;
            info!(name, "GatewayClass marked Accepted");
        }
        Err(e) => {
            // TODO: do a better job at condition reason
            let condition = Condition {
                last_transition_time: Time(Timestamp::now()),
                message: e.condition_message(),
                observed_generation: gc.metadata.generation,
                reason: GatewayClassConditionReason::InvalidParameters.to_string(),
                status: "False".into(),
                type_: GatewayClassConditionType::Accepted.to_string(),
            };

            let status_patch = json!({
                "status": {
                    "conditions": [condition],
                }
            });
            let client = &ctx.client;
            let api: Api<GatewayClass> = Api::all(client.clone());
            let name = gc.name_any();
            if ctx.is_leader() {
                api.patch_status(&name, &PatchParams::default(), &Patch::Merge(status_patch))
                    .await
                    .ok();
            }

            // TODO: conider if I actually should be returning the error here?
            return Err(e);
        }
    }

    histogram!("gc_reconcile_time").record(start.elapsed().as_secs_f32());
    Ok(Action::requeue(Duration::from_mins(5)))
}

/// The controller triggers this on reconcile errors
#[tracing::instrument(level = "warn", skip(_ctx, _gc))]
fn error_policy(_gc: Arc<GatewayClass>, e: &shared::Error, _ctx: Arc<Data>) -> Action {
    warn!("Reconcile error: {}", e);
    counter!("gc_reconciled_error").increment(1);

    Action::requeue(Duration::from_mins(1))
}

struct Data {
    client: Client,
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

impl shared::controller::Reconciler<GatewayClass, Data> for Reconciler {
    type ReconcilerFut =
        Pin<Box<dyn Future<Output = Result<Action, shared::Error>> + Send + 'static>>;
    fn reconcile(key: Arc<GatewayClass>, context: Arc<Data>) -> Self::ReconcilerFut {
        Box::pin(reconcile(key, context))
    }
}

struct ErrorPolicy;

impl shared::controller::ErrorPolicy<GatewayClass, shared::Error, Data> for ErrorPolicy {
    fn error_policy(key: Arc<GatewayClass>, error: &shared::Error, context: Arc<Data>) -> Action {
        error_policy(key, error, context)
    }
}

pub fn controller(
    client: Client,
    config: Config,
) -> (
    Store<GatewayClass>,
    watch::Receiver<bool>,
    impl Future<Output = Result<(), shared::Error>>,
) {
    info!("Initializing 'GatewayClass' Controller...");

    let gateway = Api::<GatewayClass>::all(client.clone());
    let controller = BFallController::new(
        client.clone(),
        config,
        gateway,
        "pangolin-gateway-controller",
    );

    let store = controller.store();
    let lease_details = controller.lease_details();

    (store, lease_details, async move {
        controller
            .run::<Reconciler, ErrorPolicy, Data>(Data {
                client,
                leader_status: None,
            })
            .await
    })
}
