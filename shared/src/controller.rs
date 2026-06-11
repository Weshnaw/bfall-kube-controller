use std::{sync::Arc, time::Duration};

use futures::{StreamExt, TryFuture};
use kube::{
    Api, Client, Resource,
    runtime::{
        Controller,
        controller::{Action, Config},
        reflector::Store,
        watcher,
    },
};
use kube_leader_election::{LeaseLock, LeaseLockParams, LeaseLockResult};
use metrics::gauge;
use serde::de::DeserializeOwned;
use tokio::{
    select,
    sync::{Notify, watch},
};
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

pub struct BFallController<K>
where
    K: Clone + Resource + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone + std::fmt::Debug + Default + Unpin,
{
    exit_tx: Option<Arc<Notify>>,
    leader_rx: watch::Receiver<bool>,
    controller: Controller<K>,
}

impl<K> BFallController<K>
where
    K: Clone + Resource + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone + std::fmt::Debug + Default + Unpin,
{
    pub fn new(client: Client, config: Config, main_api: Api<K>, lease_name: &str) -> Self {
        info!("Initializing client...");

        let lease_namespace = std::env::var("NAMESPACE").unwrap_or("default".into());

        let (leader_tx, leader_rx) = watch::channel(false);
        let leader_guage = gauge!("leader_status");
        leader_guage.set(0);

        let holder_id = std::env::var("HOSTNAME").unwrap_or(Uuid::new_v4().to_string());
        let leadership = LeaseLock::new(
            client,
            &lease_namespace,
            LeaseLockParams {
                holder_id: holder_id.clone(),
                lease_name: lease_name.into(),
                lease_ttl: Duration::from_secs(15),
            },
        );
        let exit_tx = Arc::new(Notify::new());

        let exit_rx = exit_tx.clone();
        tokio::spawn(async move {
            info!("Starting leader election id='{holder_id}'...");
            loop {
                match leadership.try_acquire_or_renew().await {
                    Ok(LeaseLockResult::Acquired(_)) => {
                        debug!("Lease acquired...");
                        leader_guage.set(1);
                        leader_tx.send(true).ok();
                    }
                    Ok(_) => {
                        debug!("Unable to acquire lease...");
                        leader_guage.set(0);
                        leader_tx.send(false).ok();
                    }
                    Err(e) => {
                        warn!("failed to acquire or renew leadership: {}", e);
                        leader_guage.set(0);
                        leader_tx.send(false).ok();
                    }
                }
                select! {
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                    _ = exit_rx.notified() => break,
                }
            }
            leadership.step_down().await.ok();
        });

        Self {
            exit_tx: Option::Some(exit_tx),
            controller: Self::create_controller(config, main_api, leader_rx.clone()),
            leader_rx,
        }
    }

    pub fn with_shared_lease(
        config: Config,
        main_api: Api<K>,
        leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            exit_tx: None,
            controller: Self::create_controller(config, main_api, leader_rx.clone()),
            leader_rx,
        }
    }

    fn create_controller(
        config: Config,
        main_api: Api<K>,
        mut leader_rx: watch::Receiver<bool>,
    ) -> Controller<K> {
        Controller::new(main_api, watcher::Config::default())
            .with_config(config)
            .shutdown_on_signal()
            .graceful_shutdown_on(async move {
                leader_rx.wait_for(|&val| !val).await.ok();
                debug!("Leadership lost, restarting controller...");
            })
    }

    pub fn owns<Child>(mut self, child: Api<Child>) -> Self
    where
        Child: Resource<DynamicType = ()>
            + Clone
            + DeserializeOwned
            + std::fmt::Debug
            + Send
            + 'static,
    {
        self.controller = self.controller.owns(child, watcher::Config::default());
        self
    }

    pub fn store(&self) -> Store<K> {
        self.controller.store()
    }

    pub fn lease_details(&self) -> watch::Receiver<bool> {
        self.leader_rx.clone()
    }

    pub async fn run<R, EP, Ctx>(mut self, mut context: Ctx) -> Result<(), crate::Error>
    where
        R: Reconciler<K, Ctx>,
        R::ReconcilerFut: TryFuture<Ok = Action> + Send + 'static,
        EP: ErrorPolicy<K, <R::ReconcilerFut as TryFuture>::Error, Ctx>,
        <R::ReconcilerFut as TryFuture>::Error: std::error::Error + Send + 'static,
        Ctx: CheckLeadershipStatus,
    {
        context.set_leader(self.leader_rx.clone());
        let context = Arc::new(context);
        self.leader_rx.wait_for(|&val| val).await.unwrap();

        info!("Starting controller...");
        self.controller
            .run(R::reconcile, EP::error_policy, context.clone())
            .for_each(|res| async move {
                match res {
                    Ok(o) => trace!("reconciled {:?}", o),
                    Err(e) => trace!("reconcile failed: {}", e),
                }
            })
            .await;

        info!("Controller terminated...");
        if let Some(exit_tx) = self.exit_tx {
            exit_tx.notify_waiters();
        }

        if *self.leader_rx.borrow() {
            Ok(())
        } else {
            Err(crate::Error::LostLeadership)
        }
    }
}

pub trait ErrorPolicy<K, E, Ctx> {
    fn error_policy(key: Arc<K>, error: &E, context: Arc<Ctx>) -> Action;
}

pub trait Reconciler<K, Ctx> {
    type ReconcilerFut;
    fn reconcile(key: Arc<K>, context: Arc<Ctx>) -> Self::ReconcilerFut;
}

pub trait CheckLeadershipStatus {
    fn is_leader(&self) -> bool;
    fn set_leader(&mut self, status: watch::Receiver<bool>);
}
