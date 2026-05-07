#![allow(dead_code)]

use futures::{StreamExt, stream};
use gateway_api::apis::experimental::gatewayclasses::GatewayClass;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use serde::Deserialize;
use shared::pangolin::PangolinClient;
use tracing::{debug, info};

use crate::crd::PangolinConfig;

#[derive(Debug, Clone)]
pub struct PangolinResourceConfig {
    api: PangolinApiConfig,
    org: String,
    visibility: Visibility,
    sites: Vec<String>,
    listeners: Vec<Listener>,
}

impl PangolinResourceConfig {
    pub fn new(
        api: PangolinApiConfig,
        org: String,
        visibility: Visibility,
        sites: Vec<String>,
        listeners: Vec<Listener>,
    ) -> Self {
        PangolinResourceConfig {
            api,
            org,
            visibility,
            sites,
            listeners,
        }
    }

    pub fn listeners(&self) -> &Vec<Listener> {
        &self.listeners
    }

    fn create_client(&self) -> PangolinClient {
        PangolinClient::new(&self.api.api_endpoint, &self.api.api_key, &self.org)
    }

    pub async fn check_resource(&self) -> Result<(), shared::Error> {
        let client = self.create_client();

        // TODO: handle situation where *.subdomain.basedomain.url is being used but pangolin only defines
        //       basedomain.url, this should still be valid but is not being covered here
        // Check base domain exists
        if !stream::iter(&self.listeners)
            .all(|listener| async {
                client
                    .domain_exists(listener.tld())
                    .await
                    .unwrap_or_default()
            })
            .await
        {
            return Err(shared::Error::Validate(
                shared::ValidateError::DomainsNotValid,
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PangolinApiConfig {
    api_endpoint: String,
    api_key: String,
}

impl PangolinApiConfig {
    pub async fn from_gatewayclass(
        client: &Client,
        gc: &GatewayClass,
    ) -> Result<Self, shared::Error> {
        debug!("Retrieving params_ref");
        let params_ref = gc
            .spec
            .parameters_ref
            .as_ref()
            .ok_or(shared::Error::MissingObjectKey(
                "gatewayclass.spec.parameters_ref",
            ))?;

        let config_name = &params_ref.name;
        info!("Retrieving PangolinConfig: {}", config_name);
        let config_api: Api<PangolinConfig> = Api::all(client.clone());
        let config = config_api.get(config_name).await?;

        Self::from_config(client, &config).await
    }

    pub async fn from_config(
        client: &Client,
        config: &PangolinConfig,
    ) -> Result<Self, shared::Error> {
        // Fetch the Secret referenced by the config
        let secret_name = &config.spec.api_key_ref.name;
        let secret_ns = config
            .spec
            .api_key_ref
            .namespace
            .as_deref()
            .unwrap_or(client.default_namespace());
        debug!("Retrieving Secret: {}:{}", secret_ns, secret_name);
        let secrets: Api<Secret> = Api::namespaced(client.clone(), secret_ns);
        let secret = secrets.get(secret_name).await?;

        debug!("Retrieving Data");

        let api_endpoint = config.spec.api.clone();
        let api_key = str::from_utf8(
            &secret
                .data
                .ok_or(shared::Error::MissingObjectKey("secret.spec.data"))?
                .get(&config.spec.api_key_ref.key)
                .ok_or(shared::Error::MissingObjectKey(
                    "secret.spec.data.{secret_key}",
                ))?
                .0,
        )?
        .to_string();

        Ok(Self {
            api_endpoint,
            api_key,
        })
    }

    pub async fn check_endpoint(self) -> Result<(), shared::Error> {
        let response = reqwest::Client::new()
            .get(format!("{}/v1/", self.api_endpoint))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?
            .json::<Status>()
            .await?;

        if response.message.to_lowercase() == "healthy" {
            Ok(())
        } else {
            Err(shared::Error::ApiServerUnhealthy)
        }
    }
}

#[derive(Deserialize, Debug)]
struct Status {
    message: String,
}

#[derive(Debug, Clone)]
pub struct Listener {
    port: i32,
    protocol: Protocol,
    tld: String,
    wildcard: bool,
}

impl Listener {
    pub fn new(port: i32, protocol: Protocol, tld: String, wildcard: bool) -> Self {
        Self {
            port,
            protocol,
            tld,
            wildcard,
        }
    }

    pub fn is_valid_domain(&self, hostname: impl AsRef<str>) -> bool {
        if self.wildcard {
            hostname.as_ref().ends_with(&format!(".{}", self.tld))
        } else {
            self.tld == hostname.as_ref()
        }
    }

    pub fn tld(&self) -> &String {
        &self.tld
    }
}

#[derive(Debug, Clone)]
pub enum Protocol {
    Https,
    Http, // Disables the SSL on pangolin for this address
}

impl Protocol {
    pub fn from_str(label: impl AsRef<str>) -> Self {
        match label.as_ref().to_ascii_lowercase().as_str() {
            "http" => Self::Http,
            _ => Self::Https, // TODO: should probably do proper invalid checks here
        }
    }
}

#[derive(Debug, Clone)]
pub enum Visibility {
    Public,
    Private,
}

impl Visibility {
    pub fn from_str(label: impl AsRef<str>) -> Option<Self> {
        match label.as_ref().to_ascii_lowercase().as_str() {
            "public" => Some(Self::Public),
            "private" => Some(Self::Private),
            _ => None,
        }
    }
}
