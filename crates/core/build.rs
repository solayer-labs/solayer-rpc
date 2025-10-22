// build.rs
use std::env;

fn env_flag(name: &str) -> bool {
    env::var_os(name).is_some()
}

fn main() {
    // Re-run the build script when these change
    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_MAINNET");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_DEVNET");

    let is_release = env::var("PROFILE").is_ok_and(|p| p == "release");

    // Features are exposed as CARGO_FEATURE_<NAME>=1
    let has_mainnet = env_flag("CARGO_FEATURE_MAINNET");
    let has_devnet = env_flag("CARGO_FEATURE_DEVNET");

    if !is_release {
        // In non-release builds, default to devnet only if nothing was specified.
        if !has_mainnet && !has_devnet {
            // Use a private cfg instead of pretending a Cargo feature is enabled.
            println!("cargo:rustc-cfg=devnet_default");
        }
        return;
    }

    // In release builds, enforce exactly one feature.
    match (has_mainnet, has_devnet) {
        (true, false) => {} // OK: mainnet
        (false, true) => {} // OK: devnet
        (false, false) => {
            panic!("Release build requires exactly one feature: use `--features mainnet` or `--features devnet`.");
        }
        (true, true) => {
            panic!("Release build must not enable both `mainnet` and `devnet`.");
        }
    }
}
