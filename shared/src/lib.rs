pub mod controller;

use derive_more::{Debug, Display, Error, From};

#[derive(Debug, Display, Error, From)]
pub enum Error {
    LostLeadership,
    KubeError(kube::Error),
    LeaderElectionError(kube_leader_election::Error),
    #[from(skip)]
    MissingObjectKey(#[error(not(source))] &'static str),
    #[from(skip)]
    CouldNotCreateResource(#[error(not(source))] &'static str),
}
