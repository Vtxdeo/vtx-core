use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    println!("cargo:rerun-if-changed=build.rs");

    let bindings_code = format!(
        r###"
        wasmtime::component::bindgen!({{
            inline: r#"{content}"#,
            world: "vtx-plugin",
            imports: {{ default: async | ignore_wit }},
            exports: {{ default: async }},
            with: {{
                "vtx:api/vtx-vfs.buffer": crate::common::buffer::RealBuffer,
            }}
        }});
        "###,
        content = vtx_protocol::WIT_CONTENT
    );

    let dest_path = Path::new(&out_dir).join("host_bindings.rs");
    std::fs::write(&dest_path, bindings_code).expect("Failed to write host_bindings.rs");
}
