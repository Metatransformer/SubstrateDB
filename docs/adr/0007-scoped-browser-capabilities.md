---
status: accepted
---

# Scoped browser capabilities

As an initial design to validate, the application backend decides permissions and issues short-lived signed scoped tokens; browsers connect directly to Substrate, which enforces those scopes. Supabase provides a user-experience reference, without a Supabase, PostgreSQL, or RLS dependency. This keeps entitlement decisions in the application while Substrate provides direct connections and enforcement. Token format, immediate revocation, and renewal details remain unresolved.
