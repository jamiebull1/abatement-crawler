# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest tests/
pytest tests/test_models.py          # single file
pytest --cov=abatement_crawler tests/  # with coverage

# Lint / format
ruff check .
ruff check --fix .

# Run CLI
abatement-crawler crawl --config config/config.yaml
abatement-crawler crawl --config config/config.yaml --mode seed --seed-urls https://...
abatement-crawler crawl --config config/config.yaml --fresh   # clear URL cache first
abatement-crawler export --config config/config.yaml --format csv   # also: jsonl, parquet, markdown
abatement-crawler web --config config/config.yaml --port 5000
abatement-crawler sessions --config config/config.yaml
```

Requires `ANTHROPIC_API_KEY` environment variable.

## Architecture

The crawler collects structured **AbatementRecord** objects—data about carbon abatement measures (costs, potential, sector, geography, source provenance, quality scores)—by searching the web and extracting information via the Claude API.

### Pipeline flow

1. **QueryBuilder** (`search.py`) generates search queries from scope config (abatement_types × sectors × cost_terms × geography)
2. **SearchClient** (`search.py`) executes queries via DuckDuckGo (default) and caches results
3. **score_relevance** (`relevance.py`) filters results by keyword density and domain priors (IPCC/IEA/gov.uk scored highest)
4. **SnowballCrawler** (`snowball.py`) drives a priority-queue traversal (min-heap ordered by relevance score) up to `max_depth` and `max_total_documents` limits
5. **DocumentIngester** (`ingestion.py`) fetches and parses HTML (trafilatura), PDF (pymupdf/pdfplumber), Excel, DOCX, and JSON; also extracts outbound links
6. **LLMExtractor** (`extraction.py`) chunks large documents and calls Claude to extract structured AbatementRecord fields as JSON; periodically runs a "reflection" step where the LLM evaluates crawl coverage and suggests query improvements
7. **Normaliser** (`normalisation.py`) converts currencies to GBP, maps geographies to ISO 3166, normalises units (pint), and recalculates MAC
8. **score_quality** (`quality.py`) computes a 0–1 composite score from: evidence completeness, source type prior, peer-review status, data recency (7-year half-life), and cost data presence
9. **StorageManager** (`storage.py`) persists to SQLite with soft-delete deduplication via a `duplicate_clusters` table
10. Outbound links from each document are scored and qualifying ones are added back to the snowball priority queue

### Key modules

| Module | Responsibility |
|--------|---------------|
| `crawler.py` | Top-level orchestrator; dispatches search vs seed modes |
| `snowball.py` | Priority-queue crawl loop |
| `extraction.py` | Claude API calls + JSON parsing into `AbatementRecord` |
| `normalisation.py` | Currency, units, geography, MAC normalisation |
| `quality.py` | Quality scoring |
| `relevance.py` | Pre-fetch relevance filtering |
| `storage.py` | SQLite persistence, deduplication, session tracking |
| `export.py` | JSONL, CSV, Parquet, Markdown report generation |
| `web/app.py` | Flask web UI with `/config`, `/crawl`, `/results` routes |
| `models.py` | Pydantic `AbatementRecord` with 100+ fields |

### Configuration

`config/config.example.yaml` defines all options. Key fields: `scope` (sectors, geographies, abatement_types, year_range), `llm_model`, `max_depth`, `max_total_documents`, `relevance_threshold`, `db_path`, `output_dir`. The `--fresh` CLI flag clears the `url_cache` table before crawling.

### Linting

Ruff targets Python 3.11, 100-char line limit, rule sets E/F/W/I (E501 ignored).
