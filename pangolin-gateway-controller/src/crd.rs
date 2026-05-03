use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
// TODO should probably just move these to their own rs files
/********************** PangolinConfigSpec **********************/
/// CRD for configuring a pangolin proxy with a GatewayClass
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "bfall.me",
    version = "v1alpha1",
    kind = "PangolinConfig",
    doc = "Configuration for the Pangolin gateway controller"
)]
#[serde(rename_all = "camelCase")]
pub struct PangolinConfigSpec {
    /// fqdn of the pangolin integration api
    pub api: String,
    /// Reference to a secret containing an API authentication key
    pub api_key_ref: SecretKeyRef,
}

/// Secret reference for use with the PangolinConfigSpec
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Secret name
    pub name: String,
    /// Secret key inside the secret resource
    pub key: String,
    /// Namespace, if not present assumed to match the default namespace
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}
