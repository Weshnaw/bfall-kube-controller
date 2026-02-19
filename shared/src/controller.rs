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

pub struct BFallController<K, Child> {
    exit_tx: mpsc::Sender<()>,
    leader_status: Arc<AtomicBool>,
    leader_rx: mpsc::Receiver<bool>,
    main_api: Api<K>,
    children: Vec<Api<Child>>,
    config: Config,
}

impl<K, Child> BFallController<K, Child>
where
    K: Clone + Resource + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone + std::fmt::Debug + Default + Unpin,
    Child: Clone + DeserializeOwned + std::fmt::Debug + Send + 'static,
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

        Self {
            exit_tx,
            leader_status,
            leader_rx,
            main_api,
            children: vec![],
            config,
        }
    }

    pub fn owns(mut self, child: Api<Child>) -> Self
    where
        Child: Resource<DynamicType = ()>,
    {
        self.children.push(child);
        self
    }

    pub async fn run<R, EP, Ctx>(self, mut context: Ctx) -> Result<(), crate::Error>
    where
        R: Reconciler<K, Ctx>,
        R::ReconcilerFut: TryFuture<Ok = Action> + Send + 'static,
        EP: ErrorPolicy<K, <R::ReconcilerFut as TryFuture>::Error, Ctx>,
        <R::ReconcilerFut as TryFuture>::Error: std::error::Error + Send + 'static,
        Ctx: CheckLeadershipStatus,
        Child: Resource<DynamicType = ()>,
    {
        let should_end_loop = Arc::new(AtomicBool::new(true));
        let leader_rx = Arc::new(Mutex::new(self.leader_rx));

        context.set_leader_atomic_bool(self.leader_status);

        let context = Arc::new(context);
        loop {
            while !leader_rx.lock().await.recv().await.unwrap_or(false) {
                trace!("Waiting for status notification...");
            }

            info!("Starting controller...");
            let leader_rx_clone = leader_rx.clone();
            let should_end_loop_clone = should_end_loop.clone();
            let controller = Controller::new(self.main_api.clone(), watcher::Config::default());
            let controller = self.children.iter().fold(controller, |controller, child| {
                controller.owns(child.clone(), watcher::Config::default())
            });
            controller
                .with_config(self.config.clone())
                .shutdown_on_signal()
                .graceful_shutdown_on(async move {
                    while leader_rx_clone.lock().await.recv().await.unwrap_or(false) {}
                    debug!("Leadership lost, restarting controller...");
                    should_end_loop_clone.store(false, Ordering::SeqCst);
                })
                .run(R::reconcile, EP::error_policy, context.clone())
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
        self.exit_tx.send(()).await?;
        Ok(())
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
