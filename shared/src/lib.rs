pub mod controller;

use derive_more::{Debug, Display, Error, From};
#[derive(Debug, Display, Error, From)]
pub enum Error {
    KubeError(kube::Error),
    LeaderElectionError(kube_leader_election::Error),
    ShutdownSendError(tokio::sync::mpsc::error::SendError<()>),
    #[from(skip)]
    MissingObjectKey(#[error(not(source))] &'static str),
    #[from(skip)]
    CouldNotCreateResource(#[error(not(source))] &'static str),
}
