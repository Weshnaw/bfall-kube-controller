use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

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
    sync::{Mutex, mpsc},
};
use tracing::{debug, info, trace, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use uuid::Uuid;

pub struct BFallController<K>
where
    K: Clone + Resource + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone + std::fmt::Debug + Default + Unpin,
{
    exit_tx: mpsc::Sender<()>,
    leader_status: Arc<AtomicBool>,
    leader_rx: Arc<Mutex<mpsc::Receiver<bool>>>,
    controller: Controller<K>,
}

impl<K> BFallController<K>
where
    K: Clone + Resource + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone + std::fmt::Debug + Default + Unpin,
{
    pub async fn new(client: Client, config: Config, main_api: Api<K>) -> Self {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();

        info!("Initializing client...");

        let lease_namespace = std::env::var("NAMESPACE").unwrap_or("default".into());

        let (leader_tx, leader_rx) = mpsc::channel(32);
        let leader_status = Arc::new(AtomicBool::new(false));
        let leader_guage = gauge!("leader_status");
        leader_guage.set(0);

        let holder_id = std::env::var("HOSTNAME").unwrap_or(Uuid::new_v4().to_string());
        let leadership = LeaseLock::new(
            client,
            &lease_namespace,
            LeaseLockParams {
                holder_id: holder_id.clone(),
                lease_name: "tailscale-ingress-controller-lock".into(),
                lease_ttl: Duration::from_secs(15),
            },
        );
        let status_clone = leader_status.clone();
        let (exit_tx, mut exit_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            let status = status_clone;
            info!("Starting leader election id='{holder_id}'...");
            loop {
                match leadership.try_acquire_or_renew().await {
                    Ok(LeaseLockResult::Acquired(_)) => {
                        debug!("Lease acquired...");
                        leader_guage.set(1);
                        status.store(true, Ordering::SeqCst);
                        leader_tx.try_send(true).ok();
                    }
                    Ok(_) => {
                        debug!("Unable to acquire lease...");
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
                select! {
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                    _ = exit_rx.recv() => break,
                }
            }
            leadership.step_down().await.ok();
        });
        let leader_rx = Arc::new(Mutex::new(leader_rx));
        let leader_rx_clone = leader_rx.clone();
        let controller = Controller::new(main_api, watcher::Config::default())
            .with_config(config)
            .shutdown_on_signal()
            .graceful_shutdown_on(async move {
                while leader_rx_clone.lock().await.recv().await.unwrap_or(false) {}
                debug!("Leadership lost, restarting controller...");
            });

        Self {
            exit_tx,
            leader_status,
            leader_rx,
            controller,
        }
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

    pub async fn run<R, EP, Ctx>(self, mut context: Ctx) -> Result<(), crate::Error>
    where
        R: Reconciler<K, Ctx>,
        R::ReconcilerFut: TryFuture<Ok = Action> + Send + 'static,
        EP: ErrorPolicy<K, <R::ReconcilerFut as TryFuture>::Error, Ctx>,
        <R::ReconcilerFut as TryFuture>::Error: std::error::Error + Send + 'static,
        Ctx: CheckLeadershipStatus,
    {
        context.set_leader_atomic_bool(self.leader_status);

        let context = Arc::new(context);
        while !self.leader_rx.lock().await.recv().await.unwrap_or(false) {
            trace!("Waiting for status notification...");
        }

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
        self.exit_tx.send(()).await?;
        // I beleive there is no other way to end the loop other then some other error which should but bubbled, or we lose leadership
        Err(crate::Error::LostLeadership)
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
    fn set_leader_atomic_bool(&mut self, status: Arc<AtomicBool>);
}
