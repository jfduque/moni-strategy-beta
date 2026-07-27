fn main() {
    for proto in [
        "../../proto/moni/link/v1/link.proto",
        "../../proto/moni/store/v1/store.proto",
        "../../proto/moni/v1/common.proto",
        "../../proto/moni/v1/monitor.proto",
    ] {
        println!("cargo:rerun-if-changed={proto}");
    }
    tonic_prost_build::configure()
        .compile_protos(
            &[
                "../../proto/moni/link/v1/link.proto",
                "../../proto/moni/store/v1/store.proto",
                "../../proto/moni/v1/monitor.proto",
            ],
            &["../../proto"],
        )
        .expect("failed to compile moni protobuf definitions");
}
