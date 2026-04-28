use std::{pin::Pin, sync::Arc, time::Duration};

use gateway_api::{
    constants::{GatewayConditionReason, GatewayConditionType},
    gatewayclasses::GatewayClass,
    gateways::Gateway,
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
        reflector::{ObjectRef, Store},
    },
};
use metrics::{counter, histogram};
use serde_json::json;
use shared::controller::{BFallController, CheckLeadershipStatus};
use tokio::{sync::watch, time::Instant};
use tracing::{debug, info, warn};

use crate::CONTROLLER_NAME;

#[tracing::instrument(level = "debug", skip(ctx, gw), fields(gw.name = gw.metadata.name, gw.namespace=gw.metadata.namespace))]
async fn reconcile(gw: Arc<Gateway>, ctx: Arc<Data>) -> Result<Action, shared::Error> {
    let start = Instant::now();
    let name = gw.name_any();
    let namespace = gw
        .namespace()
        .ok_or(shared::Error::MissingObjectKey("metadata.namespace"))?;

    // Look up the GatewayClass from the local cache.
    let class_name = &gw.spec.gateway_class_name;
    let gc_ref = ObjectRef::<GatewayClass>::new(class_name);
    let Some(gc) = ctx.gc_store.get(&gc_ref) else {
        warn!(
            name,
            class = class_name,
            "GatewayClass not in cache yet, requeueing"
        );
        return Ok(Action::requeue(Duration::from_secs(5)));
    };

    // Only handle Gateways whose GatewayClass belongs to this controller.
    if gc.spec.controller_name != CONTROLLER_NAME {
        return Ok(Action::await_change());
    }
    counter!("gw_reconciled").increment(1);
    info!(name, "Reconciling GatewayClass");

    let now = Time(Timestamp::now());
    let generation = gw.metadata.generation;

    let accepted = Condition {
        last_transition_time: now.clone(),
        message: "Gateway accepted by gateway-controller".into(),
        observed_generation: generation,
        reason: GatewayConditionReason::Accepted.to_string(),
        status: "True".into(),
        type_: GatewayConditionType::Accepted.to_string(),
    };

    let programmed = Condition {
        last_transition_time: now,
        message: "Gateway is programmed".into(),
        observed_generation: generation,
        reason: GatewayConditionReason::Accepted.to_string(),
        status: "True".into(),
        type_: GatewayConditionType::Programmed.to_string(),
    };

    // TODO: check API details, and mark gateway with status if not valid
    // TODO: do more reasearch on what/if I should have an addresses field
    // TODO: actually use a non-static looked up value
    // TODO: graceful shutdown update the conditions to false
    let status_patch = json!({
        "status": {
            "conditions": [accepted, programmed],
            "addresses": [
                {
                    "type": "Hostname",
                    "value": "pangolin.bfall.me"
                }
            ]
        }
    });
    let gw_api: Api<Gateway> = Api::namespaced(ctx.client.clone(), &namespace);
    if !ctx.is_leader() {
        debug!("Skipping reconciliation as not leader");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }
    gw_api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(status_patch))
        .await?;
    info!(name, "Gateway marked Accepted");
    histogram!("gw_reconcile_time").record(start.elapsed().as_secs_f32());
    Ok(Action::requeue(Duration::from_mins(5)))
}

#[tracing::instrument(level = "warn", skip(_ctx, _gw))]
fn error_policy(_gw: Arc<Gateway>, e: &shared::Error, _ctx: Arc<Data>) -> Action {
    warn!("Reconcile error: {}", e);
    counter!("gw_reconciled_error").increment(1);
    Action::requeue(Duration::from_secs(1))
}

struct Data {
    client: Client,
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

pub fn controller(
    client: Client,
    config: Config,
    gc_store: Store<GatewayClass>,
    lease_details: watch::Receiver<bool>,
) -> (
    Store<Gateway>,
    impl Future<Output = Result<(), shared::Error>>,
) {
    info!("Initializing 'Gateway' Controller...");

    let gateway = Api::<Gateway>::all(client.clone());
    let controller = BFallController::with_shared_lease(config, gateway, lease_details);

    let store = controller.store();

    (store, async move {
        controller
            .run::<Reconciler, ErrorPolicy, Data>(Data {
                client,
                gc_store,
                leader_status: None,
            })
            .await
    })
}
