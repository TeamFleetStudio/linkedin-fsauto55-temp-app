# LinkedIn Lead Enrichment

Enriches leads in PostgreSQL database with LinkedIn profile URLs using the LinkedIn Profile Finder API (scraper-api.fsgarage.in).

## Features

- **LinkedIn Profile Finder API**: Uses real browser rendering with DuckDuckGo/Startpage
- **5 Search Strategies** (with fallback):
  1. `name` + `brand_name` (company) + `city state` (from city_state_zip)
  2. `name` + `business_name` (from metadata) + `city zip`
  3. `name` + `brand_name` only
  4. `name` + `business_name` only
  5. Custom query: `"name" company site:linkedin.com/in`

- **Safe Processing**:
  - Skips leads that already have `linkedin_url`
  - Batch processing for 100k+ records
  - Rate limiting to avoid API overload
  - Progress logging and statistics
  - Confidence scoring (high/medium/low)

## API Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `POST /search` | Find by name, company, location |
| `POST /search/custom` | Raw query search (fallback) |
| `GET /health` | Check API availability |

## Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment** (optional - defaults are set):
   ```bash
   cp .env.example .env
   # Edit .env if needed
   ```

## Usage

### Basic Usage
```bash
python enrich_linkedin.py
```

### With Options
```bash
# Process only 100 leads (for testing)
python enrich_linkedin.py --limit 100

# Start from specific offset
python enrich_linkedin.py --offset 5000

# Custom batch size
python enrich_linkedin.py --batch-size 25

# Disable fallback custom search
python enrich_linkedin.py --no-fallback
```

## Database

**Connection**: `postgres://admin:***@fshackathon.fsgarage.in:5447/enrich`

### Schema
```sql
CREATE TABLE "public"."leads" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "name" TEXT NOT NULL,
  "brand_name" TEXT NULL,
  "email" TEXT NULL,
  "linkedin_url" TEXT NULL,
  "metadata" JSONB NOT NULL DEFAULT '{}'::jsonb,
  "source" TEXT NULL,
  "source_identifier" TEXT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
```

### Expected Metadata Structure
```json
{
  "business_name": "Right Coast Brands LLC",
  "city_state_zip": "Framingham, MA 01702",
  "phone": "(571) 397-2393",
  "status": "APPROVED"
}
```

## Search Strategies

### Strategy 1: Name + Brand + City State
```json
POST /search
{
  "name": "John Smith",
  "company": "ABC Corp",
  "location": "Boston, MA"
}
```

### Strategy 2: Name + Business Name + City Zip
```json
POST /search
{
  "name": "John Smith",
  "company": "ABC Corporation LLC",
  "location": "Boston 02101"
}
```

### Strategy 3 & 4: Name + Company Only
```json
POST /search
{
  "name": "John Smith",
  "company": "ABC Corp"
}
```

### Strategy 5: Custom Query (Fallback)
```json
POST /search/custom
{
  "query": "\"John Smith\" ABC Corp site:linkedin.com/in"
}
```

## Confidence Levels

| Level | Meaning |
|-------|---------|
| high | ≥50% name parts match + company matches |
| medium | Partial name match or name without company |
| low | Weak match (URL-slug-only) |

## Files

| File | Description |
|------|-------------|
| `enrich_linkedin.py` | Main enrichment script |
| `.env` | Environment configuration |
| `.env.example` | Configuration template |
| `requirements.txt` | Python dependencies |

## Logs

Logs are written to:
- Console (INFO level)
- `enrich_leads_YYYYMMDD_HHMMSS.log` file

## Statistics Output

```
==========================================
ENRICHMENT COMPLETE
==========================================
Duration: 45.2 minutes
Processed: 1000
Found: 512
Not Found: 476
Errors: 12
Found by Strategy:
  Strategy 1: 234
  Strategy 2: 89
  Strategy 3: 102
  Strategy 4: 45
  Strategy 5: 42
Success Rate: 51.2%
```
