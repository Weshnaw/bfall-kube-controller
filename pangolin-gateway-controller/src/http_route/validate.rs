use crate::http_route::intermediate::RetrievedData;

pub async fn validate_against_pangolin_api(data: &RetrievedData) -> Result<(), shared::Error> {
    // TODO: should collect all errors instead of failing on the first
    // TODO: do all the checks in parallel
    // TODO: consider using something like tokio Samaphore in case of connection / concurrency limits
    // TODO: move this check on the gateway level instead of per route, and then we only check if the gateway is valid
    for hostname in data.hosts_iter() {
        hostname.pangolin_server().check_resource().await?;
    }

    for (hostname, rule) in data.rules_iter() {
        hostname.check_rule(rule)?;
    }

    Ok(())
}
