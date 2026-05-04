use kube::Client;
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
            std::env::set_var("RUST_LOG", "warn,pangolin_newt_init=info");
        }
    }

    let _client = Client::try_default().await?;

    // TODO:
    // 1. check for an existing config file; ends early if it exists
    // 2. get pangolin details from env
    // 3. check if a site already exists with the matching name, and if it is running or not
    // 3.1 if it does not exist create it
    // 3.2 if it does exist and is offline; use the existing site's credentials
    // 3.3 if it does exist and is already online; append 01,02,03... (this could be an optional flag to fail fast or append the 01)
    // 4. grab the credentials for the site
    // 5. upload it to /config/config.json

    Ok(())
}
