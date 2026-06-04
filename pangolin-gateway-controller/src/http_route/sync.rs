use crate::intermediate::RetrievedData;

pub fn update_pangolin_api(data: &RetrievedData) -> Result<(), shared::Error> {
    // TODO: consider using the blueprint API, then the validation is handled by pangolin
    for (hostname, rule) in data.combined_iter() {
        hostname.apply_rule(rule)?;
    }

    Ok(())
}

pub fn update_kube_statuses() -> Result<(), shared::Error> {
    todo!()
}
