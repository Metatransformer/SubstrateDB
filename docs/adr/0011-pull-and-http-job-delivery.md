---
status: accepted
---

# Pull workers and HTTP job delivery

Substrate supports both connected application workers that pull jobs and signed HTTP delivery to application routes. Both execute business logic outside the Substrate runtime. Supporting two delivery modes adds protocol and lifecycle work but serves both persistent workers and on-demand application functions without requiring a separate queue product. HTTP acknowledgement rules, signing details, timeouts, retry configuration, and concurrency limits remain to be specified.
