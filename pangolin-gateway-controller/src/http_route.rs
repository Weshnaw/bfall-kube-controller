use std::{pin::Pin, sync::Arc, time::Duration};

use futures::{StreamExt, stream};
use gateway_api::{
    gatewayclasses::GatewayClass,
    gateways::Gateway,
    httproutes::{HTTPRoute, HttpRouteParentRefs},
};
use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, Client, ResourceExt,
    runtime::{
        controller::{Action, Config},
        reflector::{ObjectRef, Store},
    },
};
use metrics::{counter, histogram};
use shared::controller::{BFallController, CheckLeadershipStatus};
use tokio::{sync::watch, time::Instant};
use tracing::{debug, info, warn};

use crate::crd::PangolinConfig;

#[tracing::instrument(level = "debug", skip(ctx, hr), fields(hr.name = hr.metadata.name, hr.namespace=hr.metadata.namespace))]
async fn reconcile(hr: Arc<HTTPRoute>, ctx: Arc<Data>) -> Result<Action, shared::Error> {
    let client = &ctx.client;
    let start = Instant::now();
    counter!("hr_reconciled").increment(1);
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
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    let hostnames = hr
        .spec
        .hostnames
        .as_deref()
        .ok_or(shared::Error::MissingObjectKey("spec.hostnames"))?;

    // TODO: add warning if there a hostname does not match any gateway ref
    let _hostname_gateway: Vec<(&String, &PangolinApiConfig)> = hostnames
        .iter()
        .filter_map(|hostname| {
            let config = pangolin_configs.iter().find(|cfg| {
                cfg.listeners.iter().any(|listener| {
                    if listener.wildcard {
                        hostname.ends_with(&format!(".{}", listener.tld))
                    } else {
                        &listener.tld == hostname
                    }
                })
            })?;

            Some((hostname, config))
        })
        .collect();

    let rules = hr
        .spec
        .rules
        .as_deref()
        .ok_or(shared::Error::MissingObjectKey("spec.rules"))?;

    let _path_prefix = rules.iter().filter_map(|rule| {
        // TODO: double check if there is some way to specify HTTP/HTTPS/H2C for the backend address
        //       we currently are just assuming its http
        let backends = rule.backend_refs.as_deref()?.iter().filter_map(|backend| {
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

            Some((fqdn, port))
        });
        // TODO: transform this into (http/https), {name}.{namespace}, port
        // TODO: map kubernetes fqdn

        let matches = rule.matches.as_deref()?.iter().filter_map(|m| {
            let path = m.path.as_ref()?;

            let match_type = path.r#type.as_ref()?;
            let match_path = path.value.as_ref()?;

            Some((match_type, match_path))
        });
        // TODO: add warning if invalid matches are used
        // I.E Pangolin currently only supporst path matches
        // NOTE: for now we assume it strips the the path prefix,
        //       but there is a filters section which has details for rewriting the path
        // TODO: do research and add details for the filters section
        //       additionally there is backend filters as well

        Some((backends, matches))
    });

    // TODO: check if there are any pangolin conflicts

    // Do API Updates
    if !ctx.is_leader() {
        debug!("Skipping reconciliation as not leader");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }
    // TODO: send the updates to pangolin
    histogram!("hr_reconcile_time").record(start.elapsed().as_secs_f32());
    Ok(Action::requeue(Duration::from_mins(5)))
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
    let visibility =
        infra_labels
            .get("bfall.me/pangolin-visibility")
            .and_then(|label| match label.to_ascii_lowercase().as_str() {
                "public" => Some(Visibility::Public),
                "private" => Some(Visibility::Private),
                _ => None,
            })?;
    let site = infra_labels.get("bfall.me/pangolin-site").cloned()?;

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
            let protocol = match listener.protocol.to_ascii_lowercase().as_str() {
                "http" => Protocol::Http,
                _ => Protocol::Https, // TODO: should probably do proper invalid checks here
            };

            let hostname = listener.hostname.as_deref()?;

            let (wildcard, tld) = parse_domain(hostname)?;

            Some(Listener {
                port,
                protocol,
                tld,
                wildcard,
            })
        })
        .collect();

    info!(
        "Obtained Pangolin API details from Gateway: {}:{}",
        gw_namespace, &gw_name
    );
    Some(PangolinApiConfig {
        api_endpoint,
        org,
        visibility,
        site,
        api_key,
        listeners,
    })
}

#[derive(Debug)]
#[allow(dead_code)]
struct PangolinApiConfig {
    api_endpoint: String,
    api_key: String,
    org: String,
    visibility: Visibility,
    site: String,
    listeners: Vec<Listener>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Listener {
    port: i32,
    protocol: Protocol,
    tld: String,
    wildcard: bool,
}

#[derive(Debug)]
enum Protocol {
    Https,
    Http, // Disables the SSL on pangolin for this address
}

#[derive(Debug)]
enum Visibility {
    Public,
    Private,
}

/// The controller triggers this on reconcile errors
#[tracing::instrument(level = "warn", skip(_ctx, _svc))]
fn error_policy(_svc: Arc<HTTPRoute>, e: &shared::Error, _ctx: Arc<Data>) -> Action {
    warn!("Reconcile error: {}", e);
    counter!("hr_reconciled_error").increment(1);
    // TODO: apply error status
    Action::requeue(Duration::from_secs(1))
}

struct Data {
    client: Client,
    gw_store: Store<Gateway>,
    gc_store: Store<GatewayClass>,
    leader_status: Option<watch::Receiver<bool>>,
}

impl CheckLeadershipStatus for Data {
    fn is_leader(&self) -> bool {
        self.leader_status
            .as_ref()
            .is_some_and(|status| *status.borrow())
    }

    fn set_leader(&mut self, status: watch::Receiver<bool>) {
        self.leader_status = Some(status);
    }
}

struct Reconciler;

impl shared::controller::Reconciler<HTTPRoute, Data> for Reconciler {
    type ReconcilerFut =
        Pin<Box<dyn Future<Output = Result<Action, shared::Error>> + Send + 'static>>;
    fn reconcile(key: Arc<HTTPRoute>, context: Arc<Data>) -> Self::ReconcilerFut {
        Box::pin(reconcile(key, context))
    }
}

struct ErrorPolicy;

impl shared::controller::ErrorPolicy<HTTPRoute, shared::Error, Data> for ErrorPolicy {
    fn error_policy(key: Arc<HTTPRoute>, error: &shared::Error, context: Arc<Data>) -> Action {
        error_policy(key, error, context)
    }
}

pub async fn controller(
    client: Client,
    config: Config,
    gw_store: Store<Gateway>,
    gc_store: Store<GatewayClass>,
    lease_details: watch::Receiver<bool>,
) -> Result<(), shared::Error> {
    info!("Initializing 'HTTPRoute' Controller...");

    let route = Api::<HTTPRoute>::all(client.clone());
    BFallController::with_shared_lease(config, route, lease_details)
        .run::<Reconciler, ErrorPolicy, Data>(Data {
            client,
            gw_store,
            gc_store,
            leader_status: None,
        })
        .await
}

pub fn parse_domain(input: &str) -> Option<(bool, String)> {
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
