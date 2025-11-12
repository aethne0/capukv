use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
};

use clap::Parser;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// raft peer-id
    #[arg(long)]
    id: String,

    /// grpc port
    #[arg(short, long, default_value_t = 5701)]
    port: u16,

    /// frontend port
    #[arg(long, default_value_t = 5700)]
    api_port: u16,

    #[arg(short, long)]
    dir: String,

    /// uri to contact to join cluster
    #[clap(long, value_delimiter = ',', value_parser)]
    peers: Option<Vec<String>>,

    /// ids of peers
    #[clap(long, value_delimiter = ',', value_parser)]
    peer_ids: Option<Vec<String>>,
}

#[cfg(debug_assertions)]
fn init_logging() {
    let filter =
        filter::Targets::new().with_default(LevelFilter::OFF).with_targets(vec![("capukv", LevelFilter::DEBUG)]);
    //.with_targets(vec![("lsm_tree", LevelFilter::TRACE), ("fjall", LevelFilter::TRACE)]);
    // let fmt_layer =
    //     fmt::layer().with_file(false).with_line_number(false).with_target(false).with_ansi(true).without_time();
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
    assert_eq!(args.peers.iter().count(), args.peer_ids.iter().count(), "Differing numbers of peers, peer_ids");
    let peers = args
        .peers
        .unwrap_or(vec![])
        .into_iter()
        .zip(args.peer_ids.unwrap_or(vec![]))
        .filter(|(_, id)| *id != args.id)
        .map(|(uri, id)| {
            // We convert the id from String->Uuid->String to ensure its a valid Uuid and that its
            // in canonical form. User can include/not-include the hyphens etc.
            // Past here we just use type: String instead, as usually its being ser/de anyway.
            let id = Uuid::from_str(&id).expect("Invalid Uuid?").to_string();
            capukv::ArgPeer { uri, id }
        })
        .collect();

    init_logging();

    let runtime =
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Couldn't initialize runtime");

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.port));
    let uri = format!("http://{}", addr.to_string());
    // todo haha
    let frontend_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.api_port));
    let frontend_addr_str = format!("{}", frontend_addr.to_string());
    let frontend_uri = format!("http://{}", frontend_addr.to_string());

    tracing::info!("CapuKV starting...");
    tracing::info!("Raft gRPC uri: ---------- {}", uri);
    tracing::info!("Api gRPC uri:  ---------- {}", frontend_uri);
    tracing::info!("Log dir: ----------- {}", &args.dir);

    let dir = std::path::Path::new(&args.dir);
    runtime.block_on(async move {
        capukv::CapuKv::build_and_run(args.id, dir, addr, peers, frontend_addr_str).await.unwrap();
    });

    Ok(())
}
