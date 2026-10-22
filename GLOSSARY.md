# SubstrateDB

## Language

**Substrate runtime** — The service responsible for data, messaging, subscriptions, and scheduling.

**Application worker** — An application-owned process that connects to Substrate to execute jobs. It is distinct from a Substrate cluster node.

**Job** — A unit of application work awaiting or undergoing execution.

**Schedule** — A rule determining when jobs become due.

**Live query subscription** — A subscription keeping a query's current result up to date.

**Durable event subscription** — A subscription consuming retained events with a recoverable position, including events missed while disconnected.

**Named vector field** — A document field containing an embedding for a particular representation, such as its title or image.

**Search visibility** — The state in which a committed document revision is available to search queries.

**Namespace** — An isolated grouping of Substrate data and messaging resources with its own access scope and resource limits. Application tenants may exist within a namespace.

**Schedule occurrence** — One intended firing of a schedule at a particular scheduled time, distinct from any retries of the resulting job.
