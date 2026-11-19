fn main() {
    // Ensure SQLx's embedded migration macro is re-evaluated when migrations change.
    println!("cargo:rerun-if-changed=migrations");
}
