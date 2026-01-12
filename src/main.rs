
use tracing::info;
use tracing_subscriber::{prelude::*, fmt, EnvFilter};

fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    info!("Hello, world!");
}
