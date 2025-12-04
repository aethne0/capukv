use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// uris of all members (including self)
    /// Expected cluster size is the count of these passed in members
    #[clap(short, long, value_delimiter = ',', value_parser)]
    peer_uris: Vec<tonic::transport::Uri>,

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
        capukv::CapuKv::build_and_run(args.dir, args.raft_addr, args.api_addr, args.peer_uris).await.unwrap();
    });

    Ok(())
}
