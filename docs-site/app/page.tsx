import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "SubstrateDB Documentation",
  alternates: { canonical: "/" },
};

const capabilities = [
  {
    number: "01",
    title: "Documents",
    text: "Nested JSON, indexed queries, revision compare-and-set, pagination, and atomic command batches.",
  },
  {
    number: "02",
    title: "Durable work",
    text: "Pull workers, signed HTTP delivery, leases, retries, dead letters, ordering keys, and cron schedules.",
  },
  {
    number: "03",
    title: "Hybrid search",
    text: "Named dense and sparse fields, BM25, exact and experimental graph ANN, filters, and visibility barriers.",
  },
  {
    number: "04",
    title: "Realtime",
    text: "Document watches, indexed live queries, retained events, channels, presence, and scoped browser capabilities.",
  },
  {
    number: "05",
    title: "Cache primitives",
    text: "TTL values, counters, hashes, lists, sets, sorted sets, rate limits, and fenced leases in the same runtime.",
  },
  {
    number: "06",
    title: "Replicated clusters",
    text: "Fixed shard groups, explicit placement, quorum reads and writes, leader failover, and operator-led membership changes.",
  },
];

export default function HomePage() {
  return (
    <div className="landing-page">
      <section className="hero">
        <div className="hero-grid" aria-hidden="true" />
        <div className="hero-copy">
          <div className="eyebrow"><span /> Self-hosted application substrate</div>
          <h1>One runtime for data,<br /><em>work, and realtime.</em></h1>
          <p className="hero-lede">
            SubstrateDB is a Rust runtime for JSON documents, durable jobs, schedules, cache primitives, vector and keyword search, and browser WebSockets.
          </p>
          <div className="hero-actions">
            <Link className="button button-primary" href="/quickstart/">Start the quickstart <span aria-hidden="true">→</span></Link>
            <Link className="button button-secondary" href="/overview/">Read the overview</Link>
          </div>
          <div className="status-banner">
            <span className="status-icon" aria-hidden="true">i</span>
            <p><strong>Development status.</strong> The implementation is functional and failure-tested, but it is not a production-qualified database. The approximate vector index and sustained production-scale operation remain experimental or unqualified.</p>
          </div>
        </div>

        <div className="quickstart-card">
          <div className="terminal-header">
            <div className="terminal-dots" aria-hidden="true"><span /><span /><span /></div>
            <span>Get running locally</span>
            <span>Rust 1.87+</span>
          </div>
          <pre><code><span className="code-comment"># Clone and build the runtime</span>{"\n"}<span className="code-prompt">$</span> git clone https://github.com/Metatransformer/SubstrateDB.git{"\n"}<span className="code-prompt">$</span> cd SubstrateDB{"\n"}<span className="code-prompt">$</span> cargo build --release --bin substrate{"\n\n"}<span className="code-comment"># Start a loopback-only development server</span>{"\n"}<span className="code-prompt">$</span> ./target/release/substrate --dev</code></pre>
          <div className="terminal-footer">
            <span><i /> Runtime: <strong>127.0.0.1:7432</strong></span>
            <Link href="/quickstart/">Full setup guide <span aria-hidden="true">↗</span></Link>
          </div>
        </div>
      </section>

      <section className="capabilities-section" aria-labelledby="capabilities-title">
        <div className="section-heading">
          <div>
            <span className="section-kicker">Integrated primitives</span>
            <h2 id="capabilities-title">Fewer seams in your application stack.</h2>
          </div>
          <p>Application workers still execute your code. SubstrateDB stores and coordinates the data and work around it in one deployable runtime.</p>
        </div>
        <div className="capability-grid">
          {capabilities.map((capability) => (
            <article className="capability-card" key={capability.title}>
              <span className="capability-number">{capability.number}</span>
              <h3>{capability.title}</h3>
              <p>{capability.text}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="runtime-section">
        <div>
          <span className="section-kicker">Deployment boundary</span>
          <h2>The docs are hosted. Your database is not.</h2>
        </div>
        <p>This Vercel deployment serves static documentation only. Run SubstrateDB locally or on your own persistent server; the database runtime does not run inside this site or a Vercel serverless function.</p>
        <Link href="/operations/">Plan a runtime deployment <span aria-hidden="true">→</span></Link>
      </section>
    </div>
  );
}
