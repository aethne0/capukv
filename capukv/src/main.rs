use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;
use hickory_resolver::{IntoName, Resolver};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// uris of all members (including self)
    /// Expected cluster size is the count of these passed in members
    #[clap(short, long, value_delimiter = ',', value_parser)]
    peer_uris: Option<Vec<tonic::transport::Uri>>,

    // todo these are handled correctly by clap groups
    /// Expected node count
    #[arg(long)]
    expect: Option<usize>,

    /// dns string to lookup for peers (SRV)
    #[arg(long)]
    dns: Option<String>,

    /// this node's uri for redirects
    #[arg(long)]
    redirect_uri: Option<String>,

    /// raft grpc uri
    #[arg(short, long)]
    raft_addr: SocketAddr,

    /// api grpc uri
    #[arg(short, long)]
    api_addr: SocketAddr,

    #[arg(short, long)]
    dir: PathBuf,
}

#[cfg(debug_assertions)]
fn init_logging() {
    let filter =
        filter::Targets::new().with_default(LevelFilter::OFF).with_targets(vec![("capukv", LevelFilter::DEBUG)]);
    let fmt_layer = fmt::layer().with_file(true).with_line_number(true).with_target(false).with_ansi(true);
    tracing_subscriber::registry().with(fmt_layer).with(filter).init();
}
#[cfg(not(debug_assertions))]
fn init_logging() {
    let filter =
        filter::Targets::new().with_default(LevelFilter::OFF).with_targets(vec![("capukv", LevelFilter::INFO)]);
    let fmt_layer = fmt::layer().with_file(false).with_line_number(false).with_target(false);
    tracing_subscriber::registry().with(fmt_layer).with(filter).init();
}

fn main() -> Result<(), capukv::Error> {
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Couldn't initialize runtime");

    tracing::info!("CapuKV starting...");
    tracing::info!("Raft gRPC uri: ---------- {}", args.raft_addr);
    tracing::info!("Api gRPC uri:  ---------- {}", args.api_addr);
    tracing::info!("Log dir: ----------- {}", args.dir.to_string_lossy());

    runtime.block_on(async move {
        init_logging();

        let peers = if let Some(peers) = args.peer_uris {
            peers
        } else {
            let resolver = Resolver::builder_tokio().expect("couldn't build dns resolver").build();

            let mut peers = vec![];
            let dns = args.dns.expect("--dns");

            loop {
                let Ok(resp) = resolver.srv_lookup(&dns).await else { continue };

                let x: Result<(), String> = async {
                    for r in resp.as_lookup().record_iter() {
                        let rx = r.data().clone().into_srv().map_err(|e| e.to_string())?;
                        let port = rx.port();
                        // todo error handle, retry i guess
                        let target_resp = resolver
                            .lookup_ip(rx.target().clone().into_name().map_err(|e| e.to_string())?)
                            .await
                            .map_err(|e| e.to_string())?;
                        let ip = if let Some(ip) = target_resp.as_lookup().records().first() {
                            ip.data().clone().into_a().map_err(|e| e.to_string())?
                        } else {
                            continue;
                        };

                        peers.push(
                            tonic::transport::Uri::builder()
                                .scheme("http")
                                .authority(format!("{}:{}", ip.to_string(), port.to_string()))
                                .path_and_query("/")
                                .build()
                                .map_err(|e| e.to_string())?,
                        );
                    }

                    Ok(())
                }
                .await;

                if let Err(e) = x {
                    tracing::error!(e);
                }

                let expect = args.expect.expect("--expect");
                tracing::info!("DISCOVERY: DNS {} found {}/{}: {:?}", dns, peers.len(), expect, peers);
                if peers.len() == expect {
                    break peers;
                } else {
                    peers = vec![];
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

        capukv::CapuKv::build_and_run(args.dir, args.raft_addr, args.api_addr, peers).await.unwrap();
    });

    Ok(())
}
