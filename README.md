# abatement-crawler
Webcrawler to find data on carbon abatement measures

## Abatement measure format

Each abatement measure is stored as an `AbatementRecord` with the following fields, grouped by category.

### Identity

| Field | Type | Description |
|-------|------|-------------|
| `record_id` | string | Auto-generated UUID for the record |
| `measure_name` | string | Human-readable name of the abatement measure |
| `measure_slug` | string | URL-friendly slug derived from `measure_name` |
| `abatement_category` | string | One of: `fuel_switch` \| `efficiency` \| `behaviour` \| `carbon_capture` \| `process_change` \| `material_sub` |

### Scope mapping

| Field | Type | Description |
|-------|------|-------------|
| `sector` | string | High-level sector (e.g. `steel`, `transport`) |
| `sub_sector` | string | More specific sub-sector (optional) |
| `asset_type` | string \| null | Type of physical asset the measure applies to (optional) |
| `process` | string \| null | Specific industrial process (optional) |
| `scope_tag` | string | GHG accounting scope: `scope_1` \| `scope_2` \| `scope_3` \| `multiple` |

### Geography & time

| Field | Type | Description |
|-------|------|-------------|
| `geography` | string | Country or region the data applies to |
| `geography_notes` | string \| null | Clarifying notes on the geographic scope (optional) |
| `publication_year` | integer | Year the source document was published |
| `data_year` | integer \| null | Year the underlying data refers to (optional) |

### Carbon performance

| Field | Type | Description |
|-------|------|-------------|
| `abatement_potential_tco2e` | number \| null | Annual abatement potential in tCO₂e (optional) |
| `abatement_unit` | string | Unit for `abatement_potential_tco2e` (e.g. `tCO2e/yr`) |
| `abatement_percentage` | number \| null | Percentage emissions reduction relative to baseline (optional) |
| `baseline_description` | string \| null | Description of the emissions baseline (optional) |
| `carbon_intensity_baseline` | number \| null | Carbon intensity before the measure (optional) |
| `carbon_intensity_post` | number \| null | Carbon intensity after the measure (optional) |

### Cost data

| Field | Type | Description |
|-------|------|-------------|
| `capex` | number \| null | Capital expenditure (optional) |
| `capex_unit` | string \| null | Unit for `capex` (optional) |
| `capex_notes` | string \| null | Clarifying notes on CAPEX (optional) |
| `opex_fixed` | number \| null | Fixed operating expenditure (optional) |
| `opex_variable` | number \| null | Variable operating expenditure (optional) |
| `opex_unit` | string \| null | Unit for OPEX fields (optional) |
| `opex_delta` | number \| null | Incremental operating cost vs baseline (optional) |
| `lifetime_years` | integer \| null | Expected asset/measure lifetime in years (optional) |
| `discount_rate` | number \| null | Discount rate used, as a decimal (e.g. `0.035`) (optional) |
| `mac` | number \| null | Marginal abatement cost in `currency`/tCO₂e (optional) |
| `mac_notes` | string \| null | Clarifying notes on the MAC figure (optional) |
| `currency` | string | ISO 4217 currency code (default `GBP`) |
| `price_base_year` | integer \| null | Base year for all cost figures (optional) |

### Enabling conditions

| Field | Type | Description |
|-------|------|-------------|
| `dependencies` | list[string] | Technologies or conditions required for deployment |
| `co_benefits` | list[string] | Non-carbon benefits of the measure |
| `barriers` | list[string] | Known barriers to implementation |
| `implementation_complexity` | string | `low` \| `medium` \| `high` |
| `lead_time_years` | number \| null | Typical time from decision to deployment in years (optional) |

### Source provenance

| Field | Type | Description |
|-------|------|-------------|
| `source_url` | string | URL of the source document |
| `source_title` | string | Title of the source document |
| `source_type` | string | One of: `academic` \| `government` \| `consultancy` \| `ngo` \| `industry_body` \| `company_report` \| `technology_catalogue` |
| `source_organisation` | string | Organisation that produced the source |
| `authors` | list[string] | Author names |
| `doi` | string \| null | Digital Object Identifier (optional) |
| `retrieved_date` | string | ISO 8601 date the document was retrieved |
| `full_text_restricted` | boolean | Whether the full source text is paywalled or restricted |

### Quality

| Field | Type | Description |
|-------|------|-------------|
| `quality_score` | number | Composite quality score in the range 0–1 |
| `quality_flags` | list[string] | Machine-generated quality warning flags |
| `evidence_type` | string | One of: `modelled` \| `empirical` \| `expert_elicitation` \| `literature_review` |
| `peer_reviewed` | boolean | Whether the source is peer-reviewed |

### Extraction metadata

| Field | Type | Description |
|-------|------|-------------|
| `extraction_method` | string | Method used to extract the record (default `llm_structured`) |
| `extraction_confidence` | number | Extractor confidence score in the range 0–1 |
| `raw_excerpt` | string | Verbatim text from the source that supports the cost/carbon figures |
| `notes` | string \| null | Free-text notes from the extractor (optional) |
