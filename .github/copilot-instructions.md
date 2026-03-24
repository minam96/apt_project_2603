# Copilot Instructions

## Project Overview

This repository is a Korean real-estate dashboard built around:

- `index.html`: main single-file frontend dashboard
- `calc.html`: separate finance calculator page
- `server.js`: Node.js BFF that proxies and enriches public-data responses
- `scripts/*.py`: local dataset generation for subway stations and apartment coordinates

The app combines MCP-based real estate queries, direct `data.go.kr` API calls, KAPT enrichment,
buildingHub lookups, VWorld parcel fallback, and local generated JSON datasets.

## Core Working Rules

- Treat `server.js`, `index.html`, and `calc.html` as high-risk large files.
- Prefer narrow, surgical edits over broad rewrites.
- Do not reorganize large sections unless the user explicitly asks for a refactor.
- Preserve existing data flow, cache behavior, and UI rendering patterns unless the task requires a change.
- Never expose `.env` secrets to the browser.
- Keep generated files and local raw datasets out of versioned source unless the user explicitly asks otherwise.

## Important Runtime Facts

- Primary backend entrypoint: `node server.js`
- Vite dev server: `npm run dev`
- Production build: `npm run build`
- Syntax check:
  - `node --check server.js`
  - `python -m py_compile scripts/build_station_dataset.py`
  - `python -m py_compile scripts/build_apartment_coordinate_index.py`

## Data Source Model

- MCP stdio server in `_ref_real-estate-mcp` is the main trade/rent source
- `data.go.kr` raw APIs are used for direct XML/JSON fetches
- buildingHub provides building register fields such as site area and current FAR
- KAPT provides apartment directory and apartment basic/detail information
- VWorld is used as a parcel/PNU fallback for business-analysis matching
- `data/generated/*.json` provides local station and apartment coordinate datasets

## Business Analysis Hotspots

When working on the business-analysis tab:

- Official/confirmed analysis requires `zoning`, `legalFarLimit`, and `currentFar`
- Rows without the full official set must remain `estimated`, not silently treated as confirmed
- KAPT and VWorld are fallback enrichment paths, not the primary source of truth
- Shared KAPT site-area data may need proportional allocation across related rows

## UI Hotspots

- Dashboard tabs are implemented in a single HTML file with inline CSS and JS
- Many controls are filter-heavy and table-heavy, so accessibility regressions are easy
- Dynamic loading, status text, summary cards, and badges should stay understandable without color alone

## Local Instruction Files

Apply these workspace-specific instruction files together with this baseline:

- `.github/instructions/taming-copilot.instructions.md`
- `.github/instructions/dashboard-accessibility.instructions.md`

## Documentation Expectations

- If behavior, setup, data dependencies, or environment requirements change, update the relevant docs
- Prefer documenting architecture and handoff context in `docs/` or `tasks/` rather than scattering notes in code comments
