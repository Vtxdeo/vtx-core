use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let protocol_wit_path = vtx_protocol::get_wit_path();

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", protocol_wit_path.display());

    let wit_content = fs::read_to_string(&protocol_wit_path)
        .expect("Failed to read WIT definition from vtx-protocol");

    let bindings_code = format!(
        r###"
        wasmtime::component::bindgen!({{
            inline: r#"{content}"#,
            world: "plugin",
            async: true,
            with: {{
                "vtx:api/stream-io/buffer": crate::common::buffer::RealBuffer,
            }}
        }});
        "###,
        content = wit_content
    );

    let dest_path = Path::new(&out_dir).join("host_bindings.rs");
    fs::write(&dest_path, bindings_code).expect("Failed to write host_bindings.rs");
}
