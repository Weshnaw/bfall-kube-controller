#[cfg(feature = "controller")]
pub mod controller;
#[cfg(feature = "pangolin")]
pub mod pangolin;

use std::{env::VarError, str::Utf8Error};

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
    NoSitesFound,
    SiteAlreadyExists,
    NewtIdNotGenerated,
    ResourceNotFound,
    ResourceNotAccepted,
    NewtSecretNotGenerated,
    ApiServerUnhealthy,
    FetchError(FetchError),
    Validate(ValidateError),
    Utf8Error(Utf8Error),
    #[from(skip)]
    MissingObjectKey(#[error(not(source))] &'static str),
    #[from(skip)]
    CouldNotCreateResource(#[error(not(source))] &'static str),
}

impl Error {
    pub fn condition_message(&self) -> String {
        match self {
            Error::KubeError(error) => format!("Failed to access kubernetes resource: {}", error),
            Error::ReqwestError(error) => format!("Unable to access server: {}", error),
            Error::ApiServerUnhealthy => "API Server is Unhealthy".into(),
            Error::MissingObjectKey(key) => format!("Failed to find object key: {}", key),
            _ => "Internal Server Error".into(),
        }
    }
}

#[derive(Debug, Display, Error, From)]
pub enum FetchError {
    NoValidConfigs,
}

#[derive(Debug, Display, Error, From)]
pub enum ValidateError {
    DomainsNotValid,
}
