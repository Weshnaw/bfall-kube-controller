// TODO
use std::time::Duration;

use kube::{Client, runtime::controller::Config};
use pangolin_gateway_controller::{
    gateway_class_controller, gateway_controller, http_route_controller,
};

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[tokio::main]
async fn main() -> Result<(), shared::Error> {
    if std::env::var("RUST_LOG").is_err() {
        // We are just setting a default RUST_LOG value race conditions don't really matter here
        unsafe {
            std::env::set_var("RUST_LOG", "warn,pangolin_gateway_controller=info");
        }
    }

    let client = Client::try_default().await?;
    let config = Config::default().debounce(Duration::from_secs(5));

    let (gw, gwc, hr) = tokio::join!(
        gateway_controller(client.clone(), config.clone()),
        gateway_class_controller(client.clone(), config.clone()),
        http_route_controller(client.clone(), config.clone())
    );
    gw?;
    gwc?;
    hr?;
    Ok(())
}

// TODO:
// 1. Watch for Gateway and HTTPRoutes
// 1.1 Needs 3 watchers; GatewayClass, Gateway, HttpRoute
// 2. When an HTTPRoute is created/modified/deleted send an api request to the pangolin api
