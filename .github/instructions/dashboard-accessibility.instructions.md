---
applyTo: 'index.html,calc.html'
description: 'Accessibility rules for the dashboard UI, filters, tables, buttons, tabs, and status messaging.'
---

# Dashboard Accessibility Instructions

## Scope

Apply these rules whenever editing dashboard UI in:

- `index.html`
- `calc.html`

## Core Principles

- Prefer native HTML semantics over custom ARIA when native elements already fit the job.
- Keep all interactive controls keyboard reachable and visibly focused.
- Do not rely on color alone for important meaning such as loading, success, warning, feasibility, or unavailable status.
- Preserve readable layouts at narrow widths, especially for filters, summary cards, and tables.

## Buttons, Inputs, and Filters

- Every actionable button must have clear visible text or an explicit accessible name.
- Inputs and selects should have labels when they are added or significantly reworked.
- Placeholder text must not be the only source of instruction if the field is important.
- Icon-only or short-label controls must include `title` and/or `aria-label` when needed.

## Tabs and Dynamic Panels

- When changing tab behavior, preserve proper button semantics and predictable keyboard focus flow.
- If tab markup is expanded in future work, prefer `aria-selected`, `aria-controls`, and panel association.
- Loading and error states in dynamic panels should be expressed in text, not just style changes.

## Tables

- Keep header cells as real `th` elements.
- When touching table generation, preserve readable column names and consistent status text.
- For complex or business-critical tables, prefer adding captions or nearby explanatory text rather than making the UI visually denser.

## Status, Badges, and Chips

- Feasibility pills, source badges, and summary status cards must remain understandable without color perception.
- If new badge types are added, ensure they have distinct text labels.
- Warning or estimate states should always include human-readable wording.

## Responsive Behavior

- Avoid introducing fixed-width layouts that break at 320px to 480px widths.
- Let filter controls wrap instead of forcing horizontal overflow where possible.
- If a data table must overflow horizontally, keep the rest of the page usable without two-dimensional scrolling.

## Safe Defaults For This Repo

- Keep visible focus styles intact.
- Preserve contrast for dark-theme text, borders, and status pills.
- Avoid removing outlines, labels, or helper text for aesthetic cleanup alone.
