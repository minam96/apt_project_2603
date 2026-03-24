---
applyTo: '**'
description: 'Project-specific guardrails for making minimal, safe edits in large core files.'
---

# Taming Copilot For This Repo

## Primary Goal

Make the smallest correct change that solves the requested problem without causing unrelated drift.

## Non-Negotiables

- Follow direct user instructions first.
- Verify time-sensitive facts with tools instead of guessing.
- Prefer concise reasoning and direct edits over speculative redesign.

## High-Risk Files

These files are large and stateful. Treat them as surgical-edit zones:

- `server.js`
- `index.html`
- `calc.html`

## Edit Policy

- Do not reformat or reorder large sections unless explicitly requested.
- Do not replace a working flow with a new pattern just because it looks cleaner.
- Reuse existing helper functions and rendering patterns before introducing new abstractions.
- Extract helpers only when they clearly reduce duplication or unblock correctness.
- Avoid opportunistic cleanup in untouched areas.

## Data and API Safety

- `server.js` is the backend source of truth for API keys, proxying, caching, and enrichment.
- Never move secret-bearing logic into frontend code.
- Preserve cache semantics unless the task is specifically about freshness or invalidation.
- Treat `MCP -> raw API -> buildingHub/KAPT/VWorld -> local generated data` as the current integration order.

## Validation Expectations

After relevant changes:

- Run `node --check server.js` after backend edits
- Run `npm run build` after frontend/UI changes that can affect bundling
- Run targeted verification for the edited flow instead of unrelated broad testing when speed matters

## Documentation Rule

If behavior or setup changed, update docs or handoff notes in the same task whenever practical.
