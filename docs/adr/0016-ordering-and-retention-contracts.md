---
status: accepted
---

# Partition ordering and independent retention contracts

Events have partition-local ordering rather than a cluster-wide total order; jobs can request sequential execution by ordering key. Expired event history produces an explicit gap result rather than silent skipping. Pending jobs have a separate lifetime, ending only on acknowledgement, cancellation, or configured expiry; capacity pressure rejects or throttles submissions instead of silently losing accepted work. These contracts trade global ordering and indefinite replay simplicity for scalable execution and explicit recovery without tying unfinished work to event-history retention. Retention defaults and ordered-job failure behavior remain open.
