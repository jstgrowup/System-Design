# IRCTC Backend

## Service Documentation Convention

When asked to document a service in this repo (e.g. "document the X service", "write a report for Y like the api-gateway one"), follow this convention without needing further instructions. Reference examples: `api-gateway/docs/README.md`, `notification-service/docs/README.md`.

- **Location**: `<service>/docs/README.md`. Not a root-level `readme.md`.
- **Structure, in this order**:
  1. Overview — what the service is for, in 3-5 bullet points
  2. Architecture — one ASCII diagram showing this service's place among the others
  3. File Structure — annotated directory tree
  4. Lifecycle walkthroughs — 2-3 concrete, step-by-step examples of a request/message flowing through the system (happy path, an edge case, a failure path)
  5. Component Breakdown — one subsection per file/module, with the _actual current_ code pasted in (re-read the file, never paraphrase or reconstruct from memory)
  6. Environment Variables — every var actually read from `process.env`, flagging any that are unused
  7. A reference table for whatever this service's "codes" are (HTTP error codes, Kafka topics, job types, etc.)
  8. Quick Start — commands to install and run it, plus one concrete example request/message
  9. Debugging Tips — symptom → likely cause, based on what the code actually does
  10. Known Issues & Inconsistencies — anything odd found while reading (dead code, unused deps, naming mismatches, silent failure paths)
- **Tone**: plain English, one idea per sentence, no unexplained jargon. Explain the "why," not just the "what" — code already shows the what.
- **Never fix anything while documenting.** Only describe current behavior and list oddities under Known Issues — even obvious bugs. Documentation and code changes are separate tasks unless explicitly asked to fix.
- Every code snippet must match the current source exactly.

## Root README Maintenance

`README.md` at the repo root is the single source of truth for how the whole system fits together (architecture, cross-service flows, current known-broken paths). Keep it in sync with reality:

- **Whenever a change affects a flow described in `README.md`** — a new route, a fixed or newly-broken cross-service path, a new Kafka topic/producer/consumer, a new service, or anything else the root README currently describes — update the relevant section of `README.md` as part of that same change.
- **Whenever something is added that a zero-context reader would need to know** to understand the system (a new service, a new major capability, a resolved or newly-discovered gap), add it to `README.md` rather than leaving it undocumented.
- Keep the same tone and format already established there: plain language, diagrams (Mermaid) over prose where possible, and honest about what currently works vs. what doesn't.
- This is separate from the per-service `docs/README.md` convention below — the root README is the high-level map; per-service docs are the deep dive.

## TypeScript & Type Safety

**Avoid Type Erasure**

- Never use `any` — use `unknown` for unpredictable data and narrow it safely.
- Avoid `as CustomType` assertions except at external/legacy boundaries.
- Use type guards (`is` predicates, `in` checks, Zod) for runtime safety.
- Handle `null`/`undefined` explicitly — never use `!`.

**Design Patterns**

- Model complex UI/state machines with Discriminated Unions.
- Prefer `readonly` on arrays and properties in pure logic.
- Use `interface` for structural objects/class APIs; `type` for unions, intersections, and primitives.
- Use built-in utilities (`Pick`, `Omit`, `Partial`, `ReturnType`) over duplicate types.

**Execution Practices**

- Design types/interfaces _before_ writing logic.
- Explicitly type function definitions and public API returns — don't rely on inference alone.
- Co-locate types with the code that uses them; move to `types/` only if 3+ modules share them.
- Keep helpers localized; use `export type` over full imports to optimize bundling.

## Key constraints

- **Ask before building** — clarify requirements, edge cases, and scope with counter-questions before starting any implementation.
- **Docs are mandatory** — see Documentation section.
- **Never run git operations autonomously** — no `git add`, `git commit`, `git push`, or branch creation unless explicitly asked.
