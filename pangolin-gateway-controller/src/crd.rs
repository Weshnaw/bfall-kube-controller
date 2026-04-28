use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "bfall.me",
    version = "v1alpha1",
    kind = "PangolinConfig",
    doc = "Configuration for the Pangolin gateway controller"
)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct PangolinConfigSpec {
    pub api: String,
    pub api_key_ref: SecretKeyRef,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    pub name: String,
    pub key: String,
}
