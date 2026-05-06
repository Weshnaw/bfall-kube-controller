use std::sync::Arc;

use futures::{StreamExt, stream};
use gateway_api::apis::experimental::{
    gatewayclasses::GatewayClass,
    gateways::Gateway,
    httproutes::{HTTPRoute, HttpRouteParentRefs},
};
use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::{Api, ResourceExt, api::ListParams, runtime::reflector::ObjectRef};
use shared::FetchError;
use tracing::{debug, error, info, warn};

use crate::{
    crd::PangolinConfig,
    http_route::{
        Data,
        intermediate::{Backend, HostUpdate, Match, RetrievedData, Rule},
    },
    pangolin::{Listener, PangolinApiConfig, Protocol, Visibility},
};

pub async fn fetch_kubernetes_data(
    hr: Arc<HTTPRoute>,
    ctx: Arc<Data>,
) -> Result<RetrievedData, shared::Error> {
    let client = &ctx.client;
    let gateway_refs = hr
        .spec
        .parent_refs
        .as_deref()
        .ok_or(shared::Error::MissingObjectKey("spec.parent_refs"))?;
    // TODO: handle unimplemented fields with status warnings
    let pangolin_configs: Vec<_> = stream::iter(gateway_refs)
        .filter_map(|gw_ref| async {
            retrieve_pangolin_api_details(gw_ref, hr.as_ref(), ctx.as_ref()).await
        })
        .collect()
        .await;

    if pangolin_configs.is_empty() {
        warn!("No valid pangolin configs could be created...");
        return Err(shared::Error::FetchError(FetchError::NoValidConfigs));
    }

    let hostnames = hr
        .spec
        .hostnames
        .as_deref()
        .ok_or(shared::Error::MissingObjectKey("spec.hostnames"))?;

    // TODO: add warning if there a hostname does not match any gateway ref
    let hostnames = hostnames
        .iter()
        .filter_map(|hostname| {
            let config = pangolin_configs.iter().find(|cfg| {
                cfg.listeners()
                    .iter()
                    .any(|listener| listener.is_valid_domain(hostname))
            })?;

            debug!(?config);

            Some(HostUpdate::new(hostname.clone(), config.clone()))
        })
        .collect::<Vec<_>>();

    let rules = hr
        .spec
        .rules
        .as_deref()
        .ok_or(shared::Error::MissingObjectKey("spec.rules"))?;

    let rules = rules
        .iter()
        .filter_map(|rule| {
            // TODO: double check if there is some way to specify HTTP/HTTPS/H2C for the backend address
            //       we currently are just assuming its http
            let backends = rule
                .backend_refs
                .as_deref()?
                .iter()
                .filter_map(|backend| {
                    let fqdn = format!(
                        "{}.{}.svc.cluster.local",
                        backend.name,
                        backend
                            .namespace
                            .as_deref()
                            .unwrap_or(client.default_namespace())
                    );
                    // TODO: handle filter logic for things like path rewriting
                    let port = backend.port?;

                    Some(Backend::new(fqdn, port))
                })
                .collect::<Vec<_>>();

            let matches = rule
                .matches
                .as_deref()?
                .iter()
                .filter_map(|m| {
                    let path = m.path.as_ref()?;

                    let match_type = path.r#type.as_ref()?;
                    let match_path = path.value.as_ref()?;

                    Some(Match::new(match_type.clone(), match_path.clone()))
                })
                .collect::<Vec<_>>();
            // TODO: add warning if invalid matches are used
            // I.E Pangolin currently only supporst path matches
            // NOTE: for now we assume it strips the the path prefix,
            //       but there is a filters section which has details for rewriting the path
            // TODO: do research and add details for the filters section
            //       additionally there is backend filters as well

            Some(Rule::new(backends, matches))
        })
        .collect::<Vec<_>>();

    Ok(RetrievedData::new(hostnames, rules))
}

// TODO: actually do error processing instead of just '?' all the options
async fn retrieve_pangolin_api_details(
    parent_ref: &HttpRouteParentRefs,
    hr: &HTTPRoute,
    data: &Data,
) -> Option<PangolinApiConfig> {
    let client = &data.client;
    let default_ns = client.default_namespace();
    let gw_store = &data.gw_store;
    let hr_name = hr.name_any();
    let hr_namespace = hr.metadata.namespace.as_deref();

    let gw_name = &parent_ref.name;
    let gw_namespace = parent_ref
        .namespace
        .as_deref()
        .or(hr_namespace)
        .unwrap_or(default_ns);

    let gw_ref = ObjectRef::<Gateway>::new(gw_name).within(gw_namespace);

    debug!("Retrieving gateway details: {}:{}", gw_namespace, &gw_name);
    let Some(gw) = gw_store.get(&gw_ref) else {
        warn!(
            http_route = &hr_name,
            gateway = &gw_name,
            "Gateway not in cache yet, requeueing"
        );
        return None;
    };

    let gc_store = &data.gc_store;
    let gc_name = &gw.spec.gateway_class_name;

    let gc_ref = ObjectRef::<GatewayClass>::new(gc_name);

    debug!("Retrieving gateway class details: {}", gc_name);
    let Some(gc) = gc_store.get(&gc_ref) else {
        warn!(
            http_route = &hr_name,
            gateway = &gw_name,
            gateway_class = &gc_name,
            "GatewayClass not in cache yet, requeueing"
        );
        return None;
    };

    debug!("Retrieving params_ref");
    let Some(params_ref) = &gc.spec.parameters_ref else {
        warn!("No params ref found...");
        // TODO: maybe add configured default params assigned to the deployment
        return None;
    };

    let config_name = &params_ref.name;
    info!("Retrieving PangolinConfig: {}", config_name);
    let config_api: Api<PangolinConfig> = Api::all(client.clone());
    let config = match config_api.get(config_name).await {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!("No pangolin config found: {}", e);
            return None;
        }
    };

    // Fetch the Secret referenced by the config
    let secret_name = &config.spec.api_key_ref.name;
    let secret_ns = &config
        .spec
        .api_key_ref
        .namespace
        .as_deref()
        .unwrap_or(default_ns);
    debug!("Retrieving Secret: {}:{}", secret_ns, secret_name);
    let secrets: Api<Secret> = Api::namespaced(client.clone(), secret_ns);
    let secret = match secrets.get(secret_name).await {
        Ok(s) => s,
        Err(e) => {
            warn!("No secret found: {}", e);
            return None;
        }
    };

    debug!("Retrieving Data");
    let api_endpoint = config.spec.api;
    let infra_labels = gw.spec.infrastructure.as_ref()?.labels.as_ref()?;
    let org = infra_labels.get("bfall.me/pangolin-org").cloned()?;
    let visibility = infra_labels
        .get("bfall.me/pangolin-visibility")
        .and_then(|label| Visibility::from_str(label))?;
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
        return None;
    }

    let api_key = str::from_utf8(&secret.data?.get(&config.spec.api_key_ref.key)?.0)
        .ok()?
        .to_string();

    let listeners = gw
        .spec
        .listeners
        .iter()
        .filter_map(|listener| {
            // TODO: check if domain is valid for the gateway
            if parent_ref
                .section_name
                .as_deref()
                .is_some_and(|name| name != listener.name)
            {
                return None;
            }

            // TODO: pangolin doesn't really have port/protocol options afaik, should probably warn if unknown ones are being used
            let port = listener.port;
            // TODO: consider using TCP/UDP for things like the RawRoute
            let protocol = Protocol::from_str(&listener.protocol);

            let hostname = listener.hostname.as_deref()?;

            let (wildcard, tld) = parse_domain(hostname)?;

            Some(Listener::new(port, protocol, tld, wildcard))
        })
        .collect();

    info!(
        "Obtained Pangolin API details from Gateway: {}:{}",
        gw_namespace, &gw_name
    );
    Some(PangolinApiConfig::new(
        api_endpoint,
        api_key,
        org,
        visibility,
        sites,
        listeners,
    ))
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
