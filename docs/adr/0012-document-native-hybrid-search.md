---
status: accepted
---

# Document-native hybrid search

Ordinary documents can carry multiple named vector fields, and vector search supports filtering on document metadata. Initial retrieval includes semantic vector search combined with ranked keyword search. This avoids synchronizing separate document and vector products, at the cost of integrating vector and lexical indexes into document lifecycle and resource management. Application workers supply embeddings; indexing algorithms, detailed vector feature coverage, lexical ranking, and fusion methods remain open.
