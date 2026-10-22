# Shared implementation contract (planner-owned)

All modules use `crate::model::{Command, Error, Result}` and `crate::store::Kv`.

`Command` derives Clone, Debug, Serialize, Deserialize, PartialEq and has public fields: `namespace: String`, `partition: String`, `action: String`, `payload: serde_json::Value`, `request_id: String`, `now_ms: u64`. Time and request ID are supplied before application; no nondeterministic clocks/UUID generation in module execution.

`Error` derives Debug, thiserror::Error, Serialize, Deserialize, Clone and has public `code: String`, `message: String`; constructor `Error::new(code: impl Into<String>, message: impl Into<String>)`. `Result<T> = std::result::Result<T, Error>`.

`Kv` object-safe trait: `get(&self, bucket: &str, key: &str) -> Result<Option<Value>>`; `scan(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, Value)>>`; `put(&mut self, bucket: &str, key: &str, value: &Value) -> Result<()>`; `delete(&mut self, bucket: &str, key: &str) -> Result<()>`. Storage supplied to a module is already scoped to namespace AND logical partition; modules must not bypass that boundary. `crate::store::key(parts: &[&str]) -> String` produces collision-free prefix-composable keys (hex encode each UTF-8 component + `/`). `crate::store::emit(kv: &mut dyn Kv, cmd: &Command, kind: &str, resource: &str, data: Value) -> Result<u64>` appends an event with monotonic partition-local sequence to bucket `events`, padded numeric sequence key, persists sequence in `meta/event_sequence`; event contains sequence, timestamp_ms, kind, resource, data. Call only for successful changes; transaction rollback handles failure.

Each feature module exports `pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value>` and `pub fn is_mutation(action: &str) -> bool`. Dispatch by `action` prefix: document.*, index.*, event.* -> documents; job.* -> jobs; cache.* -> cache; vector.*, search.* -> search; schedule.* -> scheduler. Unknown operations must error, never silently succeed. Module code must validate its entire payload and not panic on user input. Use JSON object documents, finite vector numbers and checked arithmetic. Module tests may use public `store::MemoryKv` implementing the same scoped Kv API; it has Default and Clone and is a test/helper in-memory map.

Foundation `Engine::open(path: impl AsRef<Path>) -> Result<Self>`; `Engine::execute(&self, cmd: &Command) -> Result<Value>` wraps validation, dedup (same request ID with different command content => conflict), all module writes and return value in one immediate-durability transaction. Read operations do not store request dedup. `Engine::execute` dispatch hooks will be connected by planner as modules land. For initial tests foundation can dispatch documents/index/event and call `documents::execute`.

Document API payloads:
- document.put {collection,id,value,expected_revision?:u64}; missing expected means upsert, 0 means create-only. Returns {id,revision,value}.
- document.get {collection,id}; returns stored {id,revision,value} or not_found.
- document.delete {collection,id,expected_revision?:u64}; returns {deleted:true,revision}.
- document.query {collection,filter?:{field: value},sort?:{field,direction:"asc"|"desc"},limit?:number,offset?:number,projection?:string[]}; dotted field paths supported; equality filters initially; returns {documents:[...],total}. Document source records in `documents`, key([collection,id]); value {id,revision,value}. Revision must not reset on delete/recreate; store revision tombstone in `revisions`.
- index.create {collection,name,fields:string[],unique?:bool}; index.list {collection}. Compound equality indexes; definition bucket `indexes` key([collection,name]); postings bucket `postings`. Query indexed eligibility exported `documents::has_index(kv,collection,fields:&[String])->Result<bool>` for subscription checks. Document mutations update all relevant postings transactionally; unique violation rollback.
- event.read {after?:u64,limit?:u64}; return {events,latest,oldest}; after older than retained floor returns history_unavailable. event.trim {through:u64} explicitly removes retained events and advances floor, NEVER pending jobs.
- document.commit {document:{collection,id,value,expected_revision?},job:{id,queue,payload,...}} atomic save and job creation; planner wires documents and jobs in same Kv transaction.

Job records stored in bucket `jobs`, key([id]); fields API contract to be defined by job WP. Search source documents use the document records above, no independent source of truth. Store indexes as derived state, observe doc revision and revalidate current records/filters before returning.

Before/after behavior inventory: existing repo only has design docs. Add the specifically assigned behavior; preserve all unrelated files and document decisions. No worker git add/commit. No refactor/reformat outside assigned sections. Append-only on shared data files.
