# IRCTC Backend

## Service Documentation Convention

When asked to document a service in this repo (e.g. "document the X service", "write a report for Y like the api-gateway one"), follow this convention without needing further instructions. Reference examples: `api-gateway/docs/README.md`, `notification-service/docs/README.md`.

- **Location**: `<service>/docs/README.md`. Not a root-level `readme.md`.
- **Structure, in this order**:
  1. Overview — what the service is for, in 3-5 bullet points
  2. Architecture — one ASCII diagram showing this service's place among the others
  3. File Structure — annotated directory tree
  4. Lifecycle walkthroughs — 2-3 concrete, step-by-step examples of a request/message flowing through the system (happy path, an edge case, a failure path)
  5. Component Breakdown — one subsection per file/module, with the *actual current* code pasted in (re-read the file, never paraphrase or reconstruct from memory)
  6. Environment Variables — every var actually read from `process.env`, flagging any that are unused
  7. A reference table for whatever this service's "codes" are (HTTP error codes, Kafka topics, job types, etc.)
  8. Quick Start — commands to install and run it, plus one concrete example request/message
  9. Debugging Tips — symptom → likely cause, based on what the code actually does
  10. Known Issues & Inconsistencies — anything odd found while reading (dead code, unused deps, naming mismatches, silent failure paths)
- **Tone**: plain English, one idea per sentence, no unexplained jargon. Explain the "why," not just the "what" — code already shows the what.
- **Never fix anything while documenting.** Only describe current behavior and list oddities under Known Issues — even obvious bugs. Documentation and code changes are separate tasks unless explicitly asked to fix.
- Every code snippet must match the current source exactly.
