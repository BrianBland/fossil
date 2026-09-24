use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use fossil::archive::{load_head_commit, publish, Publication, PublicationGate};
use fossil::bootstrap::genesis_anchor;
use fossil::format::{parse_quantity, Hash32};
use fossil::normalized::read_package;
use fossil::rpc::{serve, serve_refreshing};
use fossil::store::open_store;
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
    /// Validate a normalized export and atomically publish eligible state.
    Archive(ArchiveArgs),
    /// Build a complete block-zero export package from a genesis allocation.
    Anchor(AnchorArgs),
    /// Serve the committed archive through a small Ethereum JSON-RPC surface.
    Serve(ServeArgs),
    /// Verify the committed head and commit. Lazy objects verify when read.
    Verify(StoreArgs),
    /// Run the fixed-seed local headline benchmark.
    Benchmark(BenchmarkArgs),
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
struct BenchmarkArgs {
    /// Also write the machine-readable JSON result to this path.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Run the chunked manual benchmark for this many blocks (not run in CI).
    #[arg(long)]
    manual_blocks: Option<u64>,
    /// Blocks generated and dropped per manual benchmark chunk.
    #[arg(long, default_value_t = 1_000)]
    chunk_blocks: u64,
    /// Explicit filesystem scratch directory for manual runs (never defaults to /tmp).
    #[arg(long)]
    scratch_dir: Option<PathBuf>,
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
        Command::Anchor(args) => {
            let genesis = std::fs::read(&args.genesis).context("read genesis allocation")?;
            let block = std::fs::read(&args.block).context("read canonical block zero")?;
            let package = genesis_anchor(&genesis, &block)?;
            std::fs::write(&args.output, package).context("write genesis anchor")?;
            Ok(())
        }
        Command::Serve(args) => run_server(args).await,
        Command::Verify(args) => verify(args).await,
        Command::Benchmark(args) => benchmark(args).await,
    }
}

async fn benchmark(args: BenchmarkArgs) -> Result<()> {
    let result = fossil::benchmark::run(
        args.output.as_deref(),
        args.manual_blocks,
        args.chunk_blocks,
        args.scratch_dir.as_deref(),
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
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
    let outcome = publish(store, package, gate).await?;
    println!(
        "published generation {} through {} ({}) commit {}{}",
        outcome.generation,
        outcome.published_number,
        outcome.published_hash,
        outcome.commit,
        if outcome.idempotent {
            " [idempotent]"
        } else {
            ""
        }
    );
    Ok(())
}

async fn run_server(args: ServeArgs) -> Result<()> {
    if args.max_batch == 0 {
        return Err(anyhow!("--max-batch must be greater than zero"));
    }
    let chain_id = parse_quantity(&args.store.chain_id)?;
    let store = open(&args.store).await?;
    let publication = Arc::new(Publication::load(store.clone(), chain_id).await?);
    match args.refresh_seconds {
        Some(seconds) => {
            serve_refreshing(
                publication,
                store,
                chain_id,
                std::time::Duration::from_secs(seconds),
                args.listen,
                args.max_batch,
            )
            .await
        }
        None => serve(publication, args.listen, args.max_batch).await,
    }
}

async fn verify(args: StoreArgs) -> Result<()> {
    let chain_id = parse_quantity(&args.chain_id)?;
    let store = open(&args).await?;
    let (head, commit) = load_head_commit(store.as_ref(), chain_id).await?;
    println!(
        "verified head/commit generation {} commit {} (lazy objects verify on read; this is not a full-history audit)",
        commit.generation, head.commit.digest
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
