use crate::intermediate::RetrievedData;

pub async fn validate_against_pangolin_api(data: &RetrievedData) -> Result<(), shared::Error> {
    // TODO: should collect all errors instead of failing on the first
    // TODO: do all the checks in parallel
    // TODO: consider using something like tokio Samaphore in case of connection / concurrency limits

    for (hostname, rule) in data.rules_iter() {
        hostname.check_rule(rule)?;
    }

    Ok(())
}
