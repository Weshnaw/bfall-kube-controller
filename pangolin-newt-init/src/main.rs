use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::BufReader,
    path::Path,
};

use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
};
use pangolin_newt_init::pangolin::PangolinClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewtConfig {
    id: String,
    secret: String,
    endpoint: String,
    tls_client_cert: String,
    site_id: String,
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

    let config_path = std::env::var("CONFIG_PATH").unwrap_or("/config/config.json".to_string());
    let config_path = Path::new(&config_path);
    let force: bool = std::env::var("FORCE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_default();
    let allow_delete: bool = std::env::var("ALLOW_DELETE")
        .ok()
        .and_then(|value| value.parse().ok())
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
            update_pod_annotation(&config.site_id).await?;
            info!("Config file already has data, use FORCE=true, to overwrite it");
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

    let site = if let Some(found) = found {
        info!(site=?found, "Site exists...");
        // TODO: multiple sites can exist with the same name
        if found.online() {
            // TODO: create the site anyway
            error!(site=?found, "Site is currently online already...");
            return Err(shared::Error::SiteAlreadyExists);
        }

        if allow_delete {
            info!(site=?found, "Taking over the pre-existing site...");
            pangolin.delete_site(found.id()).await?;

            pangolin
                .create_site(&pangolin_site, Option::Some(found.nice_id().clone()))
                .await?
        } else {
            error!(site=?found, "Site exists already, and delete is not explicitly allowed...");
            return Err(shared::Error::SiteAlreadyExists);
        }
    } else {
        pangolin.create_site(&pangolin_site, None).await?
    };

    update_pod_annotation(site.nice_id()).await?;

    let config = NewtConfig {
        id: site
            .newt_id()
            .as_ref()
            .ok_or(shared::Error::NewtIdNotGenerated)?
            .clone(),
        secret: site
            .secret()
            .as_ref()
            .ok_or(shared::Error::NewtSecretNotGenerated)?
            .clone(),
        endpoint: pangolin_endpoint,
        tls_client_cert: "".to_string(),
        site_id: site.nice_id().clone(),
    };

    let json = serde_json::to_string_pretty(&config)?;
    fs::write(config_path, json)?;

    Ok(())
}

async fn update_pod_annotation(id: &String) -> Result<(), shared::Error> {
    if let Ok(pod_name) = std::env::var("POD_NAME") {
        let client = Client::try_default().await?;
        let default_ns = client.default_namespace().to_string();
        let pod_namespace = std::env::var("POD_NAMESPACE").unwrap_or(default_ns);

        let pod_api: Api<Pod> = Api::namespaced(client, &pod_namespace);
        let annotations = BTreeMap::from([("bfall.me/pangolin-site-id".to_string(), id)]);

        let patch = json!({
            "metadata": {
                "annotations": annotations
            }
        });

        pod_api
            .patch(
                &pod_name,
                &PatchParams::apply("pangolin-newt-init"),
                &Patch::Merge(&patch),
            )
            .await?;
    } else {
        warn!("Unable to get 'POD_NAME', will not update pod annotation")
    }

    Ok(())
}
