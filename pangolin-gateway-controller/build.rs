use std::{env, fs, path::Path};

use kube::CustomResourceExt;

include!("src/crd.rs");

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    let out_dir = env::var("OUT_DIR").expect("Failed to obtain 'OUT_DIR'");
    let dest_path = Path::new(&out_dir).join("crds.yaml");

    let crd = PangolinConfig::crd();
    let yaml = yaml_serde::to_string(&crd).expect("Failed to stringify CRD");
    fs::write(&dest_path, yaml).expect("Failed to write crd yaml");
}
