use std::{env, fs, path::Path};

use kube::CustomResourceExt;

include!("src/crd.rs");

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    let out_dir = env::var("OUT_DIR").expect("Failed to obtain 'OUT_DIR'");
    let dest_path = Path::new(&out_dir).join("crds.yaml");

    fs::write(
        &dest_path,
        combine_yamls(&[PangolinConfig::crd(), RawRoute::crd()]),
    )
    .expect("Failed to write crd yaml");
}

fn combine_yamls<T: Serialize>(docs: &[T]) -> String {
    docs.iter()
        .map(|doc| yaml_serde::to_string(doc).expect("Failed to stringigy yaml"))
        .collect::<Vec<_>>()
        .join("---\n")
}
