use std::sync::Arc;

use futures::{StreamExt, stream};
use gateway_api::apis::experimental::{
    gateways::Gateway,
    httproutes::{HTTPRoute, HttpRouteParentRefs},
};
use kube::{ResourceExt, runtime::reflector::ObjectRef};
use shared::FetchError;
use tracing::{debug, warn};

use crate::{
    http_route::Data,
    intermediate::{Backend, HostUpdate, Match, RetrievedData, Rule},
    pangolin::PangolinResourceConfig,
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
            retrieve_pangolin_api_details(gw_ref, hr.as_ref(), ctx.as_ref())
                .await
                .and_then(|config| {
                    if config.listeners().iter().any(|listener| {
                        gw_ref
                            .section_name
                            .as_deref()
                            .is_some_and(|name| name != listener.name())
                    }) {
                        Some(config)
                    } else {
                        None
                    }
                })
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
) -> Option<PangolinResourceConfig> {
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
    let gw_accepted = gw
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.type_ == "Accepted"))
        .map(|c| c.status == "True")
        .unwrap_or(false);

    if !gw_accepted {
        warn!(gateway = &gw_name, "Gateway is not accepted, requeueing");
        return None;
    }
    PangolinResourceConfig::from_gateway(client, &gw, &data.gc_store)
        .await
        .ok()
}
