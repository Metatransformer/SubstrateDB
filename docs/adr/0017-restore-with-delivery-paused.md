---
status: accepted
---

# Restore with delivery paused

The first production release includes consistent backup and restore. Restored queues and schedules remain paused until explicitly resumed because database rollback cannot undo external effects such as payments, emails, or webhooks. This trades automatic resumption for an opportunity to reconcile side effects before work is replayed. Backup formats, retention, point-in-time recovery, and exact restoration scope remain to be specified.
