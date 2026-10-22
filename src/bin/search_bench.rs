use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use substratedb::{model::Command, store::Engine};

#[derive(Clone, Copy)]
struct Parameters {
    count: usize,
    dims: usize,
    queries: usize,
    ef: usize,
    seed: u64,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            count: 10_000,
            dims: 32,
            queries: 100,
            ef: 128,
            seed: 42,
        }
    }
}

fn usage() -> &'static str {
    "search_bench [--count N] [--dims N] [--queries N] [--ef N] [--seed N]"
}

fn parse_parameters() -> Result<Parameters> {
    let mut parameters = Parameters::default();
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            println!("{}", usage());
            std::process::exit(0);
        }
        let value = arguments
            .next()
            .with_context(|| format!("missing value for {argument}; usage: {}", usage()))?;
        match argument.as_str() {
            "--count" => parameters.count = value.parse().context("invalid --count")?,
            "--dims" => parameters.dims = value.parse().context("invalid --dims")?,
            "--queries" => parameters.queries = value.parse().context("invalid --queries")?,
            "--ef" => parameters.ef = value.parse().context("invalid --ef")?,
            "--seed" => parameters.seed = value.parse().context("invalid --seed")?,
            _ => bail!("unknown argument {argument}; usage: {}", usage()),
        }
    }
    if parameters.count == 0 {
        bail!("--count must be greater than zero");
    }
    if parameters.dims == 0 || parameters.dims > 65_536 {
        bail!("--dims must be in 1..=65536");
    }
    if parameters.queries == 0 {
        bail!("--queries must be greater than zero");
    }
    let limit = parameters.count.min(10);
    if parameters.ef < limit || parameters.ef > 100_000 {
        bail!("--ef must be at least min(count, 10) and at most 100000");
    }
    Ok(parameters)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn component(seed: u64, row: usize, column: usize) -> f64 {
    let bits = splitmix64(
        seed ^ (row as u64).wrapping_mul(0xd6e8_feb8_6659_fd93)
            ^ (column as u64).wrapping_mul(0xa076_1d64_78bd_642f),
    );
    (bits as f64 / u64::MAX as f64) * 2.0 - 1.0
}

fn normalized_vector(seed: u64, row: usize, dims: usize) -> Vec<f64> {
    let mut vector = (0..dims)
        .map(|column| component(seed, row, column))
        .collect::<Vec<_>>();
    let magnitude = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
    for value in &mut vector {
        *value /= magnitude;
    }
    vector
}

fn query_vector(parameters: Parameters, query: usize) -> Vec<f64> {
    let row = splitmix64(parameters.seed ^ query as u64) as usize % parameters.count;
    let mut vector = normalized_vector(parameters.seed, row, parameters.dims);
    for (column, value) in vector.iter_mut().enumerate() {
        *value += component(parameters.seed ^ 0x51ed_270b, query, column) * 0.01;
    }
    let magnitude = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
    for value in &mut vector {
        *value /= magnitude;
    }
    vector
}

fn command(action: &str, payload: Value, request: u64) -> Command {
    Command {
        namespace: "search-bench".into(),
        partition: "main".into(),
        action: action.into(),
        payload,
        request_id: format!("bench-{request}"),
        now_ms: 0,
    }
}

fn execute(engine: &Engine, action: &str, payload: Value, request: &mut u64) -> Result<Value> {
    *request = request
        .checked_add(1)
        .context("request sequence overflow")?;
    engine
        .execute(&command(action, payload, *request))
        .map_err(anyhow::Error::new)
}

fn hit_ids(result: &Value) -> BTreeSet<String> {
    result["hits"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|hit| hit["id"].as_str().map(str::to_owned))
        .collect()
}

fn milliseconds(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn percentile(samples: &[f64], percentile: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn latency_summary(samples: &[f64]) -> Value {
    json!({
        "p50": percentile(samples, 0.50),
        "p95": percentile(samples, 0.95),
        "p99": percentile(samples, 0.99),
    })
}

fn temporary_database_path() -> Result<PathBuf> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "substratedb-search-bench-{}-{nonce}.redb",
        std::process::id()
    )))
}

fn run(parameters: Parameters) -> Result<Value> {
    let database_path = temporary_database_path()?;
    let engine = Engine::open(&database_path).map_err(anyhow::Error::new)?;
    let mut request = 0;
    execute(
        &engine,
        "vector.configure",
        json!({"collection":"bench","name":"embedding","dimensions":parameters.dims,"metric":"cosine"}),
        &mut request,
    )?;

    let insertion_started = Instant::now();
    for index in 0..parameters.count {
        let id = format!("v{index:020}");
        execute(
            &engine,
            "document.put",
            json!({"collection":"bench","id":id,"value":{"ordinal":index}}),
            &mut request,
        )?;
        execute(
            &engine,
            "vector.upsert",
            json!({
                "collection":"bench",
                "name":"embedding",
                "id":id,
                "vector":normalized_vector(parameters.seed,index,parameters.dims),
                "source_revision":1
            }),
            &mut request,
        )?;
    }
    let insertion_time = insertion_started.elapsed();

    let indexing_started = Instant::now();
    loop {
        let result = execute(&engine, "vector.flush", json!({}), &mut request)?;
        if result["applied"].as_u64() == Some(0) {
            break;
        }
    }
    let indexing_time = indexing_started.elapsed();

    let mut exact_latencies = Vec::with_capacity(parameters.queries);
    let mut graph_latencies = Vec::with_capacity(parameters.queries);
    let mut recall = 0.0;
    let limit = parameters.count.min(10);
    for query in 0..parameters.queries {
        let vector = query_vector(parameters, query);
        let exact_started = Instant::now();
        let exact = execute(
            &engine,
            "search.query",
            json!({"collection":"bench","name":"embedding","vector":vector,"limit":limit,"exact":true}),
            &mut request,
        )?;
        exact_latencies.push(milliseconds(exact_started.elapsed()));

        let graph_started = Instant::now();
        let graph = execute(
            &engine,
            "search.query",
            json!({"collection":"bench","name":"embedding","vector":vector,"limit":limit,"exact":false,"ef":parameters.ef}),
            &mut request,
        )?;
        graph_latencies.push(milliseconds(graph_started.elapsed()));
        let exact_ids = hit_ids(&exact);
        let graph_ids = hit_ids(&graph);
        recall += exact_ids.intersection(&graph_ids).count() as f64 / exact_ids.len() as f64;
    }

    let result = json!({
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "logical_cpus": std::thread::available_parallelism().map(|n|n.get()).unwrap_or(1),
            "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        },
        "seed": parameters.seed,
        "parameters": {
            "count": parameters.count,
            "dimensions": parameters.dims,
            "queries": parameters.queries,
            "k": limit,
            "ef": parameters.ef,
        },
        "results": {
            "recall_at_10": recall / parameters.queries as f64,
            "insertion_submission_time_ms": milliseconds(insertion_time),
            "indexing_time_ms": milliseconds(indexing_time),
            "exact_search_latency_ms": latency_summary(&exact_latencies),
            "graph_search_latency_ms": latency_summary(&graph_latencies),
        }
    });
    drop(engine);
    std::fs::remove_file(&database_path)
        .with_context(|| format!("failed to remove {}", database_path.display()))?;
    Ok(result)
}

fn main() -> Result<()> {
    let result = run(parse_parameters()?)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
