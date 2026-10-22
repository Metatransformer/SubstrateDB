---
status: accepted
---

# External job execution

Substrate owns cron scheduling and queue state, while connected application workers execute business logic. It will not initially embed arbitrary application-code execution; scripting is only possible future work. This means no bundled code execution, while keeping execution and runtime isolation outside the database.
