use std::{
    fs::{self, File},
    io::BufReader,
    path::Path,
};

use kube::Client;
use pangolin_newt_init::pangolin::PangolinClient;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct NewtConfig {
    id: String,
    secret: String,
    endpoint: String,
    tls_client_cert: String,
}

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

    // TODO: could probably add an env config for config path
    let config_path = Path::new("/config/config.json");
    let force: bool = std::env::var("FORCE")
        .ok()
        .map(|value| value.parse().ok())
        .flatten()
        .unwrap_or_default();
    if !force && config_path.exists() {
        info!("Config file exists; checking if it is populated...");

        let file = File::open(config_path)?;
        let reader = BufReader::new(file);
        if let Ok(config) = serde_json::from_reader::<_, NewtConfig>(reader)
            && !config.id.is_empty()
            && !config.secret.is_empty()
            && !config.endpoint.is_empty()
        {
            info!(
                config = ?config,
                "Config file already has data, use FORCE=true, to overwrite it"
            );
            return Ok(());
        }
    }

    let pangolin_endpoint = std::env::var("PANGOLIN_ENDPOINT")?;
    let pangolin_api = std::env::var("PANGOLIN_API_ENDPOINT")?;
    let pangolin_org = std::env::var("PANGOLIN_ORG")?;
    let pangolin_site = std::env::var("PANGOLIN_SITE_NAME").or_else(|e| {
        debug!(
            error = ?e,
            "Failed to grab 'PANGOLIN_SITE_NAME' falling back to 'POD_NAME"
        );
        std::env::var("POD_NAME")
    })?;
    let pangolin_key = std::env::var("PANGOLIN_API_KEY")?;

    let pangolin = PangolinClient::new(pangolin_api, pangolin_key, pangolin_org);

    let found = pangolin.find_site_by_name(&pangolin_site).await?;

    let (site_id, credentials) = if let Some(found) = found {
        info!(site=?found, "Site exists...");
        if found.online {
            // TODO: if it does exist and is already online; append 01,02,03... (this could be an optional flag to fail fast or append the 01)
            error!(site=?found, "Site is currently online already...");
            return Err(shared::Error::SiteAlreadyExists);
        }

        info!(site=?found, "Taking over the pre-existing site...");
        let credentials = pangolin.create_newt_credentials().await?;
        // TODO: update existing site with new credentials
        todo!();

        (found.nice_id, credentials)
    } else {
        let credentials = pangolin.create_newt_credentials().await?;
        // TODO: if it does not exist create it
        todo!();
        ("".into(), credentials)
    };

    let _client = Client::try_default().await?;
    // TODO: update the pod's annotation

    let config = NewtConfig {
        id: credentials.newt_id().clone(),
        secret: credentials.newt_secret().clone(),
        endpoint: pangolin_endpoint,
        tls_client_cert: "".to_string(),
    };

    let json = serde_json::to_string_pretty(&config)?;
    fs::write(&config_path, json)?;

    Ok(())
}
