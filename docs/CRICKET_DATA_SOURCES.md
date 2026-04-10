# Cricket Data Sources for IPL

## Overview

This project uses multiple cricket data sources depending on the use case:

### 1. Live Match Ingestion (Recurring Checkpoints)
**Primary**: ESPN Cricinfo (via public HTML scraping)  
**Fallback**: CricAPI (requires API key)

- Captures periodic match state snapshots (innings summaries)
- Used for live scoring checkpoints and Polymarket odds mapping
- See: `src/ingest/cricket/live-espncricinfo.ts`

### 2. Ball-by-Ball Reporting (Historical Analysis)
**Primary**: Cricsheet JSON archives  
**Fallback**: ESPN partial recent-deliveries window

- Generates detailed ball-by-ball timeline reports
- Used by `scripts/report/ball-odds-timeline.ts` for price timeline analysis
- Cricsheet provides complete official data but with 1-2 week publication lag

## Known Limitations

### Gap Period: Match Completion to Cricsheet Publication (~1-2 weeks)

During this period:
- ✅ **Cricsheet**: Not yet published
- ✅ **ESPN**: Exposes only recent deliveries (last ~50 balls) in their JavaScript payload
- ❌ **CricAPI Free**: Limited ball-by-ball access
- ❌ **Cricbuzz**: No public API, 403 Forbidden on direct requests
- ❌ **Other Public APIs**: No reliable free source with full coverage

### Workaround

For matches within 1-2 weeks of completion, use the `--allow-partial` flag:

```bash
pnpm report:ball-odds \
  --event-slug "<polymarket-event-slug>" \
  --commentary-url "https://www.espncricinfo.com/series/..." \
  --allow-partial
```

This will output available balls from ESPN's recent window (~50 balls typically).

### Resolution

Full data becomes available when:
1. Cricsheet publishes the match JSON file (typically 1-2 weeks after match)
2. Run the script again without `--allow-partial`:

```bash
pnpm report:ball-odds \
  --event-slug "<polymarket-event-slug>" \
  --commentary-url "https://www.espncricinfo.com/series/..."
```

## Pre-downloaded Cricsheet Data

For offline analysis, import Cricsheet data locally:

```bash
pnpm db:import-cricsheet-ipl --seasons 2024 2025 2026
```

This stores all historical IPL matches from the specified seasons in the database for `ball-odds-timeline` script consumption.

## Data Source Comparison

| Source | Coverage | Latency | Auth | Type |
|--------|----------|---------|------|------|
| **Cricsheet** | Complete (120 balls per T20) | 1-2 weeks | None | JSON |
| **ESPN (Live)** | Partial recent window (~50 balls) | Real-time | None | HTML/JS |
| **CricAPI** | Partial/Limited | Real-time | API Key | REST |
| **Cricbuzz** | Complete (if accessible) | Real-time | None | API (blocked) |

## Architecture Decision

The MVP intentionally uses public sources without authentication:
- **Live ingestion**: HTML scraping (resilient to ESPN changes, no auth needed)
- **Historical reporting**: Cricsheet archives (official source, periodic download)
- **Gap mitigation**: Partial ESPN fallback with user awareness

This keeps the system simple and avoids dependency on third-party API keys or rate limits during development.
