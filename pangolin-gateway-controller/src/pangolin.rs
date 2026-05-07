#![allow(dead_code)]

use futures::{StreamExt, stream};
use gateway_api::apis::experimental::{gatewayclasses::GatewayClass, gateways::Gateway};
use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::{
    Api, Client, ResourceExt,
    api::ListParams,
    runtime::reflector::{ObjectRef, Store},
};
use serde::Deserialize;
use shared::pangolin::PangolinClient;
use tracing::{debug, error, info, warn};

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
    pub async fn from_gateway(
        client: &Client,
        gw: &Gateway,
        gc_store: &Store<GatewayClass>,
    ) -> Result<Self, shared::Error> {
        let gw_name = gw.name_any();
        let gw_namespace = gw
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(client.default_namespace());
        let gc_name = &gw.spec.gateway_class_name;

        let gc_ref = ObjectRef::<GatewayClass>::new(gc_name);

        debug!("Retrieving gateway class details: {}", gc_name);
        let gc = gc_store
            .get(&gc_ref)
            .ok_or(shared::Error::ResourceNotFound)?;

        let gc_accepted = gc
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|conditions| conditions.iter().find(|c| c.type_ == "Accepted"))
            .map(|c| c.status == "True")
            .unwrap_or(false);

        if !gc_accepted {
            warn!(
                gateway_class = &gc_name,
                "GatewayClass is not accepted, requeueing"
            );
            return Err(shared::Error::ResourceNotAccepted);
        }

        let api = PangolinApiConfig::from_gatewayclass(client, &gc).await?;

        let infra_labels = gw
            .spec
            .infrastructure
            .as_ref()
            .ok_or(shared::Error::MissingObjectKey(
                "gateway.spec.infrastructure",
            ))?
            .labels
            .as_ref()
            .ok_or(shared::Error::MissingObjectKey(
                "gateway.spec.infrastructure.labels",
            ))?;
        let org = infra_labels.get("bfall.me/pangolin-org").cloned().ok_or(
            shared::Error::MissingObjectKey(
                "gateway.spec.infrastructure.labels.\"bfall.me/pangolin-org\"",
            ),
        )?;
        let visibility = infra_labels
            .get("bfall.me/pangolin-visibility")
            .and_then(Visibility::from_str)
            .ok_or(shared::Error::MissingObjectKey(
                "gateway.spec.infrastructure.labels.\"bfall.me/pangolin-visibility\"",
            ))?;
        let sites = infra_labels
            .get("bfall.me/pangolin-site")
            .map(|str| {
                str.split(",")
                    .map(|str| str.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let annotated_sites =
            if let Some(selector) = infra_labels.get("bfall.me/pangolin-site-selector") {
                let selector = selector.clone();
                let selector_ns = infra_labels
                    .get("bfall.me/pangolin-site-selector-ns")
                    .cloned()
                    .unwrap_or(gw_namespace.to_string());

                let pods: Api<Pod> = Api::namespaced(client.clone(), &selector_ns);
                let selector =
                    ListParams::default().labels(&format!("app.kubernetes.io/name={}", &selector));
                let found = pods.list(&selector).await.ok();

                found.map(|pods| {
                    pods.iter()
                        .filter_map(|pod| {
                            pod.metadata
                                .annotations
                                .as_ref()
                                .and_then(|ann| ann.get("bfall.me/pangolin-site-id").cloned())
                        })
                        .collect::<Vec<_>>()
                })
            } else {
                None
            }
            .unwrap_or_default();

        let sites = [&sites[..], &annotated_sites[..]].concat();

        if sites.is_empty() {
            error!("No sites found for the gateway...");
            return Err(shared::Error::NoSitesFound);
        }

        let listeners = gw
            .spec
            .listeners
            .iter()
            .filter_map(|listener| {
                // TODO: pangolin doesn't really have port/protocol options afaik, should probably warn if unknown ones are being used
                let port = listener.port;
                // TODO: consider using TCP/UDP for things like the RawRoute
                let protocol = Protocol::from_str(&listener.protocol);

                let hostname = listener.hostname.as_deref()?;

                let (wildcard, tld) = parse_domain(hostname)?;

                Some(Listener::new(
                    listener.name.clone(),
                    port,
                    protocol,
                    tld,
                    wildcard,
                ))
            })
            .collect();

        info!(
            "Obtained Pangolin API details from Gateway: {}:{}",
            gw_namespace, &gw_name
        );

        Ok(Self {
            api,
            org,
            visibility,
            sites,
            listeners,
        })
    }

    pub fn listeners(&self) -> &Vec<Listener> {
        &self.listeners
    }

    fn create_client(&self) -> PangolinClient {
        PangolinClient::new(&self.api.api_endpoint, &self.api.api_key, &self.org)
    }

    pub async fn check_resource(self) -> Result<(), shared::Error> {
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
    name: String,
    port: i32,
    protocol: Protocol,
    tld: String,
    wildcard: bool,
}

impl Listener {
    pub fn new(name: String, port: i32, protocol: Protocol, tld: String, wildcard: bool) -> Self {
        Self {
            name,
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

    pub fn name(&self) -> &String {
        &self.name
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

fn parse_domain(input: &str) -> Option<(bool, String)> {
    let (is_wildcard, host) = if let Some(rest) = input.strip_prefix("*.") {
        (true, rest)
    } else {
        (false, input)
    };

    let labels: Vec<&str> = host.split('.').collect();

    if labels.len() < 2 {
        return None;
    }

    if labels.iter().any(|label| {
        label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    }) {
        return None;
    }

    Some((is_wildcard, host.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: parameterized tests
    #[test]
    fn test_parse_domain() {
        assert_eq!(parse_domain("bfall.me"), Some((false, "bfall.me".into())));

        assert_eq!(parse_domain("*.bfall.me"), Some((true, "bfall.me".into())));

        assert_eq!(parse_domain("http://bfall.me"), None);

        assert_eq!(
            parse_domain("*.test.bfall.me"),
            Some((true, "test.bfall.me".into()))
        );

        assert_eq!(parse_domain("test.*.bfall.me"), None);
    }
}
