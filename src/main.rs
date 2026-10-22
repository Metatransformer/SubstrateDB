use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
};
use substratedb::{
    cluster::{Cluster, ClusterConfig},
    runtime::Runtime,
    store::Engine,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "substrate=info,substratedb=info".into()),
        )
        .init();
    let mut data = PathBuf::from("data/substrate.redb");
    let mut listen = "127.0.0.1:7432".to_string();
    let mut dev = false;
    let mut backup = None;
    let mut restore = None;
    let mut cluster_config = None;
    let mut unix_socket: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" => {
                data = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--data requires path"))?
                    .into()
            }
            "--listen" => {
                listen = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--listen requires address"))?
            }
            "--dev" => dev = true,
            "--unix-socket" => {
                unix_socket = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--unix-socket requires path"))?,
                )
            }
            "--backup" => {
                backup = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--backup requires path"))?,
                )
            }
            "--restore" => {
                restore = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--restore requires path"))?,
                )
            }
            "--cluster-config" => {
                cluster_config = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--cluster-config requires path"))?,
                )
            }
            "--help" | "-h" => {
                println!("substrate [--data PATH] [--listen ADDRESS] [--dev] [--cluster-config PATH] [--unix-socket PATH] [--backup PATH | --restore PATH]\nAuthentication: SUBSTRATE_ADMIN_TOKEN (24+ bytes), optional SUBSTRATE_SIGNING_KEY.\n--dev enables a fixed LOCAL-ONLY development token: substrate-local-development-token\nCluster backup/restore is an offline per-shard workflow; --backup and --restore cannot be used with --cluster-config.");
                return Ok(());
            }
            _ => anyhow::bail!("unknown argument: {arg}"),
        }
    }
    let addr: SocketAddr = listen.parse()?;
    if dev && !addr.ip().is_loopback() {
        anyhow::bail!("--dev only permits a loopback listener");
    }
    if backup.is_some() && restore.is_some() {
        anyhow::bail!("choose backup or restore");
    }
    if cluster_config.is_some() && (backup.is_some() || restore.is_some()) {
        anyhow::bail!(
            "cluster backup/restore is an offline per-shard workflow; remove --cluster-config"
        );
    }
    if cluster_config.is_none() {
        if let Some(parent) = data.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        if backup.is_some() || restore.is_some() {
            let engine = Engine::open(&data)?;
            if let Some(path) = backup {
                engine.backup(&path)?;
                println!("Backup written to {path}");
                return Ok(());
            }
            if let Some(path) = restore {
                engine.restore(&path)?;
                println!("Restore complete; queues and schedules are paused");
                return Ok(());
            }
        }
    }
    let admin = if dev {
        "substrate-local-development-token".to_string()
    } else {
        std::env::var("SUBSTRATE_ADMIN_TOKEN")
            .map_err(|_| anyhow::anyhow!("set SUBSTRATE_ADMIN_TOKEN or use --dev on loopback"))?
    };
    if admin.len() < 24 {
        anyhow::bail!("SUBSTRATE_ADMIN_TOKEN must contain at least 24 bytes");
    }
    let signing = std::env::var("SUBSTRATE_SIGNING_KEY")
        .unwrap_or_else(|_| format!("substrate-signing-v1:{admin}"));
    if signing.len() < 24 {
        anyhow::bail!("SUBSTRATE_SIGNING_KEY must contain at least 24 bytes");
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let (runtime, internal, cluster) = if let Some(path) = cluster_config {
        let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let placements: Option<BTreeMap<u64, BTreeSet<u64>>> = value
            .as_object_mut()
            .and_then(|object| object.remove("placements"))
            .map(serde_json::from_value)
            .transpose()?;
        let config: ClusterConfig = serde_json::from_value(value)?;
        if !dev && config.secret.len() < 24 {
            anyhow::bail!("cluster secret must contain at least 24 bytes outside --dev");
        }
        let cluster = if let Some(placements) = placements {
            Cluster::new_with_placement(config, placements).await?
        } else {
            Cluster::new(config).await?
        };
        let internal = cluster.router();
        (
            Runtime::new_cluster(cluster.clone(), admin, signing.into_bytes()),
            Some(internal),
            Some(cluster),
        )
    } else {
        if let Some(parent) = data.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        (
            Runtime::new(Engine::open(&data)?, admin, signing.into_bytes()),
            None,
            None,
        )
    };
    let mode = runtime.mode();
    let background = runtime.background();
    let mut router = substratedb::server::router(runtime);
    if let Some(internal) = internal {
        router = router.merge(internal);
    }
    let mut ipc = None;
    if let Some(path) = &unix_socket {
        use std::os::unix::fs::PermissionsExt;
        let listener = tokio::net::UnixListener::bind(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let router = router.clone();
        ipc = Some(tokio::spawn(
            async move { axum::serve(listener, router).await },
        ));
        tracing::info!(%path,"Substrate IPC listening");
    }
    tracing::info!(address=%listener.local_addr()?,%mode,"Substrate listening");
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
        })
        .await;
    if let Some(ipc) = ipc {
        ipc.abort();
        let _ = ipc.await;
        if let Some(path) = unix_socket {
            let _ = std::fs::remove_file(path);
        }
    }
    background.abort();
    let _ = background.await;
    if let Some(cluster) = cluster {
        cluster.shutdown().await;
    }
    result?;
    Ok(())
}
