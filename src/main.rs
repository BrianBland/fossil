use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use fossil::bootstrap::genesis_anchor;
use fossil::format::{parse_quantity, Hash32};
use fossil::normalized::read_package;
use fossil::rpc::serve_tiered;
use fossil::store::open_store;
use fossil::tiered;
use fossil::tiered::PublicationGate;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Parser)]
#[command(version, about = "Immutable historical state archive for EVM chains")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate a normalized export and atomically append it as an L0 run.
    Archive(ArchiveArgs),
    /// Fold L0 runs into levels until none remain eligible, optionally forever.
    Compact(CompactArgs),
    /// Build a complete block-zero export package from a genesis allocation.
    Anchor(AnchorArgs),
    /// Serve the committed archive through a small Ethereum JSON-RPC surface.
    Serve(ServeArgs),
    /// Verify the committed head and commit. Lazy objects verify when read.
    Verify(StoreArgs),
    /// Delete objects unreachable from the head; safe alongside writers.
    Gc(GcArgs),
    /// Measure cold GETs and bytes of account-plus-storage reads over samples.
    Probe(ProbeArgs),
}

#[derive(Args, Clone)]
struct StoreArgs {
    /// Filesystem path, file:// URI, or s3://bucket/prefix.
    #[arg(long)]
    store: String,
    /// Custom S3-compatible endpoint (for example, Cloudflare R2).
    #[arg(long, env = "FOSSIL_S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "AWS_REGION")]
    s3_region: Option<String>,
    /// Chain ID as a canonical Ethereum quantity.
    #[arg(long, default_value = "0x2105")]
    chain_id: String,
}

#[derive(Args)]
struct CompactArgs {
    #[command(flatten)]
    store: StoreArgs,
    /// Keep polling for new L0 runs at this interval instead of exiting.
    #[arg(long)]
    follow_seconds: Option<u64>,
}

#[derive(Args)]
struct GcArgs {
    #[command(flatten)]
    store: StoreArgs,
    /// Also keep this many predecessor head copies (audit history).
    #[arg(long, default_value_t = 16)]
    keep_heads: usize,
    /// Never delete objects modified more recently than this (default 3 hours;
    /// must exceed the compaction deadline and the writer refresh age).
    #[arg(long, default_value_t = tiered::GC_MIN_AGE.as_secs())]
    min_age_seconds: u64,
    /// Report what would be deleted without deleting.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Args)]
struct ProbeArgs {
    #[command(flatten)]
    store: StoreArgs,
    /// JSONL samples: {"address":"0x..","slot":"0x..","block":N}.
    #[arg(long)]
    input: PathBuf,
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
}

#[derive(serde::Deserialize)]
struct Sample {
    address: fossil::format::Address,
    slot: Hash32,
    block: u64,
}

#[derive(Args)]
struct AnchorArgs {
    /// Chain genesis JSON containing the exhaustive allocation.
    #[arg(long)]
    genesis: PathBuf,
    /// Canonical block-zero JSON-RPC response or block object.
    #[arg(long)]
    block: PathBuf,
    /// Normalized fossil-export/1 JSONL destination.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Args)]
struct ArchiveArgs {
    #[command(flatten)]
    store: StoreArgs,
    /// Normalized fossil-export/1 JSONL, optionally zstd-compressed.
    #[arg(long)]
    input: PathBuf,
    #[arg(long, value_enum)]
    gate: GateKind,
    /// NUMBER:0xHASH trusted finalized checkpoint.
    #[arg(long, required_if_eq("gate", "finalized"))]
    finalized_head: Option<String>,
    /// NUMBER:0xHASH observed canonical tip.
    #[arg(long, required_if_eq("gate", "fixed-offset"))]
    observed_tip: Option<String>,
    #[arg(long, required_if_eq("gate", "fixed-offset"))]
    offset: Option<u64>,
}

#[derive(Clone, clap::ValueEnum)]
enum GateKind {
    Finalized,
    FixedOffset,
}

#[derive(Args)]
struct ServeArgs {
    #[command(flatten)]
    store: StoreArgs,
    #[arg(long, default_value = "127.0.0.1:8545")]
    listen: SocketAddr,
    #[arg(long, default_value_t = 100)]
    max_batch: usize,
    /// Poll and atomically adopt verified heads at this interval. Disabled when omitted.
    #[arg(long)]
    refresh_seconds: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fossil=info".into()),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Archive(args) => archive(args).await,
        Command::Compact(args) => compact(args).await,
        Command::Anchor(args) => {
            let genesis = std::fs::read(&args.genesis).context("read genesis allocation")?;
            let block = std::fs::read(&args.block).context("read canonical block zero")?;
            let package = genesis_anchor(&genesis, &block)?;
            std::fs::write(&args.output, package).context("write genesis anchor")?;
            Ok(())
        }
        Command::Serve(args) => run_server(args).await,
        Command::Verify(args) => verify(args).await,
        Command::Probe(args) => probe(args).await,
        Command::Gc(args) => {
            let chain_id = parse_quantity(&args.store.chain_id)?;
            let store = open(&args.store).await?;
            let report = tiered::collect_garbage(
                store.as_ref(),
                chain_id,
                args.keep_heads,
                std::time::Duration::from_secs(args.min_age_seconds),
                args.dry_run,
            )
            .await?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
    }
}

async fn archive(args: ArchiveArgs) -> Result<()> {
    let bytes = std::fs::read(&args.input).context("read input package")?;
    let package = read_package(&bytes)?;
    let configured_chain = parse_quantity(&args.store.chain_id)?;
    if package.segment.chain_id != configured_chain {
        return Err(anyhow!("input chain ID does not match --chain-id"));
    }
    let gate = match args.gate {
        GateKind::Finalized => {
            let (number, hash) = parse_checkpoint(args.finalized_head.as_deref().unwrap())?;
            PublicationGate::Finalized { number, hash }
        }
        GateKind::FixedOffset => {
            let (observed_number, observed_hash) =
                parse_checkpoint(args.observed_tip.as_deref().unwrap())?;
            PublicationGate::FixedOffset {
                observed_number,
                observed_hash,
                offset: args.offset.unwrap(),
            }
        }
    };
    let store = open(&args.store).await?;
    tiered::publish(store.as_ref(), &package, &gate).await?;
    let last = package.segment.blocks.last().context("empty package")?;
    println!("published through {} ({})", last.number, last.hash);
    Ok(())
}

async fn compact(args: CompactArgs) -> Result<()> {
    let chain_id = parse_quantity(&args.store.chain_id)?;
    let store = open(&args.store).await?;
    loop {
        let mut commits = 0;
        while tiered::compact_once(store.as_ref(), chain_id).await? {
            commits += 1;
        }
        if commits > 0 {
            tracing::info!(commits, "compacted L0 runs");
        }
        let Some(seconds) = args.follow_seconds else {
            println!("compaction drained after {commits} commits");
            return Ok(());
        };
        tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    }
}

/// Every sample opens its own reader over its own counter, so each is fully cold.
async fn probe(args: ProbeArgs) -> Result<()> {
    use futures_util::StreamExt;
    let chain_id = parse_quantity(&args.store.chain_id)?;
    let store = open(&args.store).await?;
    let samples = std::fs::read_to_string(&args.input)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<Sample>)
        .collect::<Result<Vec<_>, _>>()?;
    let results: Vec<Result<(u64, u64)>> = futures_util::stream::iter(samples)
        .map(|sample| {
            let counting = fossil::store::CountingStore::new(store.clone());
            async move {
                let reader = tiered::Reader::open(&counting, chain_id).await?;
                reader
                    .storage(sample.address, sample.slot, sample.block)
                    .await?;
                Ok(counting.take())
            }
        })
        .buffer_unordered(args.concurrency.max(1))
        .collect()
        .await;
    let errors = results.iter().filter(|result| result.is_err()).count();
    if let Some(Err(error)) = results.iter().find(|result| result.is_err()) {
        tracing::warn!(%error, errors, "probe samples failed");
    }
    let mut gets: Vec<u64> = results.iter().flatten().map(|(gets, _)| *gets).collect();
    let mut bytes: Vec<u64> = results.iter().flatten().map(|(_, bytes)| *bytes).collect();
    gets.sort_unstable();
    bytes.sort_unstable();
    let pick = |values: &[u64], quantile: f64| {
        values
            .get(((values.len() as f64 * quantile).ceil() as usize).saturating_sub(1))
            .copied()
    };
    println!(
        "{}",
        serde_json::json!({
            "samples": gets.len(),
            "errors": errors,
            "gets_p50": pick(&gets, 0.5),
            "gets_p99": pick(&gets, 0.99),
            "gets_max": gets.last(),
            "bytes_p50": pick(&bytes, 0.5),
            "bytes_p99": pick(&bytes, 0.99),
            "bytes_max": bytes.last(),
        })
    );
    Ok(())
}

async fn run_server(args: ServeArgs) -> Result<()> {
    if args.max_batch == 0 {
        return Err(anyhow!("--max-batch must be greater than zero"));
    }
    let chain_id = parse_quantity(&args.store.chain_id)?;
    // The store lives for the whole server process.
    let store: &'static Arc<dyn fossil::store::ArchiveStore> =
        Box::leak(Box::new(open(&args.store).await?));
    serve_tiered(
        store.as_ref(),
        chain_id,
        args.refresh_seconds.map(std::time::Duration::from_secs),
        args.listen,
        args.max_batch,
    )
    .await
}

async fn verify(args: StoreArgs) -> Result<()> {
    let chain_id = parse_quantity(&args.chain_id)?;
    let store = open(&args).await?;
    let reader = tiered::Reader::open(store.as_ref(), chain_id).await?;
    println!(
        "verified tiered head through {} with {} runs ({} L0), head sha256 {} (objects verify on read; this is not a full-history audit)",
        reader.latest_block(),
        reader.run_count(),
        reader.l0_count(),
        reader.head_digest()
    );
    Ok(())
}

async fn open(args: &StoreArgs) -> Result<Arc<dyn fossil::store::ArchiveStore>> {
    open_store(
        &args.store,
        args.s3_endpoint.as_deref(),
        args.s3_region.as_deref(),
    )
    .await
}

fn parse_checkpoint(value: &str) -> Result<(u64, Hash32)> {
    let (number, hash) = value
        .split_once(':')
        .ok_or_else(|| anyhow!("checkpoint must be NUMBER:0xHASH"))?;
    let number = if number.starts_with("0x") {
        parse_quantity(number)?
    } else {
        number
            .parse()
            .context("invalid decimal checkpoint number")?
    };
    Ok((number, Hash32::from_str(hash)?))
}
