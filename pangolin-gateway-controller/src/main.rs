// TODO
use std::time::Duration;

use kube::{Client, runtime::controller::Config};
use pangolin_gateway_controller::{
    gateway_class_controller, gateway_controller, http_route_controller,
};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), shared::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    if std::env::var("RUST_LOG").is_err() {
        // We are just setting a default RUST_LOG value race conditions don't really matter here
        unsafe {
            std::env::set_var("RUST_LOG", "warn,pangolin_gateway_controller=info");
        }
    }

    let client = Client::try_default().await?;
    let config = Config::default().debounce(Duration::from_secs(5));

    let (gc_store, lease_details, gc_controller) =
        gateway_class_controller(client.clone(), config.clone());
    let (gw_store, gw_controller) = gateway_controller(
        client.clone(),
        config.clone(),
        gc_store,
        lease_details.clone(),
    );

    tokio::try_join!(
        gc_controller,
        gw_controller,
        http_route_controller(client, config, gw_store, lease_details)
    )
    .map(|_| ())
}

// TODO:
// 1. Watch for Gateway and HTTPRoutes
// 1.1 Needs 3 watchers; GatewayClass, Gateway, HttpRoute
// 2. When an HTTPRoute is created/modified/deleted send an api request to the pangolin api
