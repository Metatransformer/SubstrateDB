use std::{env, fs, path::Path};

fn collect(root: &Path, dir: &Path, entries: &mut Vec<(String, String)>) {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .expect("dashboard/dist missing: run npm --prefix dashboard ci && npm --prefix dashboard run build")
        .map(|entry| entry.expect("read dashboard asset").path())
        .collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            collect(root, &path, entries);
        } else {
            let name = path
                .strip_prefix(root)
                .unwrap()
                .to_str()
                .unwrap()
                .replace('\\', "/");
            entries.push((name, path.to_str().unwrap().to_owned()));
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=dashboard/dist");
    let root = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("dashboard/dist");
    let mut entries = Vec::new();
    collect(&root, &root, &mut entries);
    assert!(
        entries.iter().any(|(name, _)| name == "index.html"),
        "dashboard build must contain index.html"
    );
    let mut code = String::from("pub static ASSETS: &[(&str, &[u8])] = &[\n");
    for (name, path) in entries {
        code.push_str(&format!("({name:?}, include_bytes!({path:?})),\n"));
    }
    code.push_str("];\n");
    fs::write(
        Path::new(&env::var("OUT_DIR").unwrap()).join("dashboard_assets.rs"),
        code,
    )
    .unwrap();
}
