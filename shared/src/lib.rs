#[cfg(feature = "controller")]
pub mod controller;

use std::env::VarError;

use derive_more::{Debug, Display, Error, From};

#[derive(Debug, Display, Error, From)]
pub enum Error {
    LostLeadership,
    KubeError(kube::Error),
    LeaderElectionError(kube_leader_election::Error),
    IoError(std::io::Error),
    JsonError(serde_json::Error),
    EnvError(VarError),
    ReqwestError(reqwest::Error),
    SiteAlreadyExists,
    NewtIdNotGenerated,
    NewtSecretNotGenerated,
    #[from(skip)]
    MissingObjectKey(#[error(not(source))] &'static str),
    #[from(skip)]
    CouldNotCreateResource(#[error(not(source))] &'static str),
}
