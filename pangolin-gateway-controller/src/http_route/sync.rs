use crate::intermediate::RetrievedData;

pub fn update_pangolin_api(data: &RetrievedData) -> Result<(), shared::Error> {
    for (hostname, rule) in data.rules_iter() {
        hostname.apply_rule(rule)?;
    }

    Ok(())
}

pub fn update_kube_statuses() -> Result<(), shared::Error> {
    todo!()
}
