"""
LinkedIn Lead Enrichment using LinkedIn Profile Finder API
- Uses scraper-api.fsgarage.in for LinkedIn profile search
- 5 fallback search strategies
- Safe batch processing for 100k+ records
- Skips leads that already have linkedin_url
"""

import os
import time
import logging
import json
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from dotenv import load_dotenv

load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(f'enrich_leads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    # Database - postgres://admin:nuKjZljI54jqFvPb@fshackathon.fsgarage.in:5447/enrich
    DB_HOST: str = os.getenv('DB_HOST', 'fshackathon.fsgarage.in')
    DB_PORT: int = int(os.getenv('DB_PORT', 5447))
    DB_NAME: str = os.getenv('DB_NAME', 'enrich')
    DB_USER: str = os.getenv('DB_USER', 'admin')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', 'nuKjZljI54jqFvPb')
    
    # LinkedIn Scraper API
    SCRAPER_API_URL: str = os.getenv('SCRAPER_API_URL', 'https://scraper-api.fsgarage.in')
    
    # Processing settings
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', 50))
    REQUESTS_PER_SECOND: float = float(os.getenv('REQUESTS_PER_SECOND', 2.0))
    USE_FALLBACK_SEARCH: bool = os.getenv('USE_FALLBACK_SEARCH', 'true').lower() == 'true'
    
    # Timeouts
    API_TIMEOUT: int = 60  # seconds


class DatabaseManager:
    """PostgreSQL database operations"""
    
    def __init__(self, config: Config):
        self.config = config
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        conn_str = (
            f"host={self.config.DB_HOST} "
            f"port={self.config.DB_PORT} "
            f"dbname={self.config.DB_NAME} "
            f"user={self.config.DB_USER} "
            f"password={self.config.DB_PASSWORD} "
            f"sslmode=disable"
        )
        self.conn = psycopg2.connect(conn_str)
        self.conn.autocommit = False
        logger.info(f"Connected to database: {self.config.DB_HOST}:{self.config.DB_PORT}/{self.config.DB_NAME}")
        
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
            
    def get_leads_to_enrich(self, batch_size: int, offset: int = 0) -> List[Dict]:
        """Get leads without LinkedIn URL"""
        query = """
            SELECT id, name, brand_name, metadata, email, source_identifier
            FROM public.leads
            WHERE (linkedin_url IS NULL OR linkedin_url = '')
            ORDER BY created_at ASC
            LIMIT %s OFFSET %s
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (batch_size, offset))
            return [dict(row) for row in cur.fetchall()]
    
    def count_leads_to_enrich(self) -> int:
        """Count leads needing enrichment"""
        query = """
            SELECT COUNT(*) FROM public.leads
            WHERE linkedin_url IS NULL OR linkedin_url = ''
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]
    
    def count_total_leads(self) -> int:
        """Count total leads"""
        query = "SELECT COUNT(*) FROM public.leads"
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]
    
    def count_enriched_leads(self) -> int:
        """Count leads with LinkedIn URL"""
        query = """
            SELECT COUNT(*) FROM public.leads
            WHERE linkedin_url IS NOT NULL AND linkedin_url != ''
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]
    
    def update_linkedin_url(self, lead_id: str, linkedin_url: str, 
                           search_method: str = None, confidence: str = None):
        """Update single lead's LinkedIn URL"""
        query = """
            UPDATE public.leads
            SET linkedin_url = %s,
                updated_at = NOW(),
                metadata = metadata || %s::jsonb
            WHERE id = %s
        """
        enrichment_meta = json.dumps({
            'linkedin_enriched_at': datetime.now().isoformat(),
            'linkedin_search_method': search_method,
            'linkedin_confidence': confidence
        })
        
        with self.conn.cursor() as cur:
            cur.execute(query, (linkedin_url, enrichment_meta, lead_id))
        self.conn.commit()
        
    def batch_update(self, updates: List[Dict]):
        """Batch update multiple leads"""
        if not updates:
            return
            
        query = """
            UPDATE public.leads
            SET linkedin_url = %s,
                updated_at = NOW(),
                metadata = metadata || %s::jsonb
            WHERE id = %s
        """
        
        with self.conn.cursor() as cur:
            for update in updates:
                enrichment_meta = json.dumps({
                    'linkedin_enriched_at': datetime.now().isoformat(),
                    'linkedin_search_method': update.get('method', 'unknown'),
                    'linkedin_confidence': update.get('confidence', 'unknown')
                })
                cur.execute(query, (
                    update['linkedin_url'],
                    enrichment_meta,
                    update['id']
                ))
        self.conn.commit()
        logger.info(f"Batch updated {len(updates)} leads")


class LinkedInSearcher:
    """
    LinkedIn Profile Finder using scraper-api.fsgarage.in
    
    API Endpoints:
    - POST /search - Find by name, company, location
    - POST /search/custom - Raw query search
    - POST /search/batch - Batch lookup (up to 100)
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.SCRAPER_API_URL.rstrip('/')
        self.session = requests.Session()
        self.last_request_time = 0
        self.min_interval = 1.0 / config.REQUESTS_PER_SECOND
        
    def _rate_limit(self):
        """Enforce rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()
        
    def search(self, name: str, company: str = None, 
               location: str = None) -> Tuple[Optional[str], str, str]:
        """
        Search using POST /search endpoint
        
        Returns: (linkedin_url, confidence, method)
        """
        self._rate_limit()
        
        payload = {"name": name}
        if company:
            payload["company"] = company
        if location:
            payload["location"] = location
            
        try:
            resp = self.session.post(
                f"{self.base_url}/search",
                json=payload,
                timeout=self.config.API_TIMEOUT
            )
            
            if resp.status_code == 200:
                data = resp.json()
                linkedin_url = data.get('linkedin_url')
                confidence = data.get('confidence', 'unknown')
                engine = data.get('engine', 'unknown')
                
                if linkedin_url:
                    logger.debug(f"Found: {linkedin_url} (confidence: {confidence})")
                    return linkedin_url, confidence, f"search_{engine}"
                    
            elif resp.status_code == 404:
                logger.debug(f"Not found: {name}")
            else:
                logger.warning(f"API error {resp.status_code}: {resp.text[:200]}")
                
        except requests.Timeout:
            logger.warning(f"Timeout searching for: {name}")
        except Exception as e:
            logger.error(f"Search error: {e}")
            
        return None, 'none', ''
    
    def search_custom(self, query: str) -> Tuple[Optional[str], str, str]:
        """
        Search using POST /search/custom endpoint with raw query
        
        Returns: (linkedin_url, confidence, method)
        """
        self._rate_limit()
        
        try:
            resp = self.session.post(
                f"{self.base_url}/search/custom",
                json={"query": query},
                timeout=self.config.API_TIMEOUT
            )
            
            if resp.status_code == 200:
                data = resp.json()
                linkedin_url = data.get('linkedin_url')
                confidence = data.get('confidence', 'unknown')
                
                if linkedin_url:
                    return linkedin_url, confidence, 'custom_search'
                    
        except Exception as e:
            logger.error(f"Custom search error: {e}")
            
        return None, 'none', ''
    
    def search_batch(self, queries: List[Dict]) -> List[Dict]:
        """
        Batch search using POST /search/batch endpoint
        
        Args:
            queries: List of {"name": ..., "company": ..., "location": ...}
            
        Returns: List of results
        """
        self._rate_limit()
        
        try:
            resp = self.session.post(
                f"{self.base_url}/search/batch",
                json={"queries": queries},
                timeout=self.config.API_TIMEOUT * 2  # Longer timeout for batch
            )
            
            if resp.status_code == 200:
                data = resp.json()
                return data.get('results', [])
                
        except Exception as e:
            logger.error(f"Batch search error: {e}")
            
        return []
    
    def check_health(self) -> bool:
        """Check if API is available"""
        try:
            resp = self.session.get(
                f"{self.base_url}/health",
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"API Health: {data}")
                return data.get('status') == 'ok'
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        return False


class LeadEnricher:
    """Main enrichment orchestrator with 5 fallback strategies"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config)
        self.searcher = LinkedInSearcher(config)
        
        self.stats = {
            'processed': 0,
            'found': 0,
            'found_by_strategy': {i: 0 for i in range(1, 6)},
            'not_found': 0,
            'errors': 0,
            'skipped': 0,
            'start_time': None
        }
    
    def _parse_metadata(self, metadata) -> Dict:
        """Parse metadata field (can be dict or JSON string)"""
        if isinstance(metadata, dict):
            return metadata
        if isinstance(metadata, str):
            try:
                return json.loads(metadata)
            except json.JSONDecodeError:
                return {}
        return {}
    
    def _parse_location(self, city_state_zip: str) -> Tuple[str, str, str]:
        """Parse city_state_zip format: 'Framingham, MA 01702'"""
        city, state, zip_code = '', '', ''
        
        if not city_state_zip:
            return city, state, zip_code
            
        parts = city_state_zip.split(',')
        if parts:
            city = parts[0].strip()
        if len(parts) > 1:
            state_zip = parts[1].strip().split()
            if state_zip:
                state = state_zip[0]
            if len(state_zip) > 1:
                zip_code = state_zip[1]
                
        return city, state, zip_code
    
    def enrich_lead(self, lead: Dict) -> Optional[Dict]:
        """
        Enrich single lead using 5 fallback strategies
        
        Strategies:
        1. name + brand_name + city state (from city_state_zip)
        2. name + business_name (metadata) + city zip
        3. name + brand_name only
        4. name + business_name only
        5. Custom query: "name company site:linkedin.com/in"
        """
        name = lead.get('name', '').strip()
        if not name:
            self.stats['skipped'] += 1
            return None
            
        brand_name = lead.get('brand_name', '').strip()
        metadata = self._parse_metadata(lead.get('metadata', {}))
        
        business_name = metadata.get('business_name', '').strip()
        city_state_zip = metadata.get('city_state_zip', '')
        city, state, zip_code = self._parse_location(city_state_zip)
        
        # Build location strings
        location_city_state = f"{city}, {state}".strip(', ') if city else None
        location_city_zip = f"{city} {zip_code}".strip() if city else None
        
        # Define 5 search strategies
        strategies = [
            # Strategy 1: name + brand_name + city state
            {
                'num': 1,
                'name': name,
                'company': brand_name if brand_name else None,
                'location': location_city_state
            },
            # Strategy 2: name + business_name + city zip
            {
                'num': 2,
                'name': name,
                'company': business_name if business_name else None,
                'location': location_city_zip
            },
            # Strategy 3: name + brand_name only
            {
                'num': 3,
                'name': name,
                'company': brand_name if brand_name else None,
                'location': None
            },
            # Strategy 4: name + business_name only
            {
                'num': 4,
                'name': name,
                'company': business_name if business_name else None,
                'location': None
            },
        ]
        
        # Try each strategy
        for strategy in strategies:
            # Skip if no useful company data for this strategy
            if not strategy['company'] and strategy['num'] in [1, 2, 3, 4]:
                # Allow strategy 1 with location even without company
                if strategy['num'] != 1 or not strategy['location']:
                    continue
            
            linkedin_url, confidence, method = self.searcher.search(
                name=strategy['name'],
                company=strategy['company'],
                location=strategy['location']
            )
            
            if linkedin_url and confidence in ['high', 'medium']:
                self.stats['found'] += 1
                self.stats['found_by_strategy'][strategy['num']] += 1
                return {
                    'id': lead['id'],
                    'linkedin_url': linkedin_url,
                    'method': f"strategy_{strategy['num']}_{method}",
                    'confidence': confidence
                }
            
            # Small delay between strategies
            time.sleep(0.2)
        
        # Strategy 5: Custom query fallback
        if self.config.USE_FALLBACK_SEARCH:
            company = brand_name or business_name
            query_parts = [f'"{name}"']
            if company:
                query_parts.append(company)
            query_parts.append('site:linkedin.com/in')
            
            query = ' '.join(query_parts)
            linkedin_url, confidence, method = self.searcher.search_custom(query)
            
            if linkedin_url:
                self.stats['found'] += 1
                self.stats['found_by_strategy'][5] += 1
                return {
                    'id': lead['id'],
                    'linkedin_url': linkedin_url,
                    'method': 'strategy_5_custom',
                    'confidence': confidence
                }
        
        self.stats['not_found'] += 1
        return None
    
    def run(self, limit: Optional[int] = None, start_offset: int = 0):
        """Run enrichment process"""
        self.stats['start_time'] = datetime.now()
        
        try:
            self.db.connect()
            
            # Check API health
            if not self.searcher.check_health():
                logger.error("LinkedIn Scraper API is not available!")
                return
            
            # Get stats
            total_leads = self.db.count_total_leads()
            to_enrich = self.db.count_leads_to_enrich()
            already_enriched = self.db.count_enriched_leads()
            
            logger.info("=" * 60)
            logger.info("DATABASE STATUS")
            logger.info("=" * 60)
            logger.info(f"Total leads: {total_leads}")
            logger.info(f"Already enriched: {already_enriched}")
            logger.info(f"To enrich: {to_enrich}")
            logger.info("=" * 60)
            
            if limit:
                to_enrich = min(to_enrich, limit)
                logger.info(f"Limiting to {limit} leads")
            
            offset = start_offset
            batch_updates = []
            
            while offset < to_enrich + start_offset:
                leads = self.db.get_leads_to_enrich(self.config.BATCH_SIZE, offset)
                if not leads:
                    break
                
                batch_num = (offset - start_offset) // self.config.BATCH_SIZE + 1
                logger.info(f"Batch {batch_num}: Processing {len(leads)} leads (offset {offset})")
                
                for i, lead in enumerate(leads):
                    # Check if we've hit the limit
                    if limit and self.stats['processed'] >= limit:
                        break
                        
                    try:
                        result = self.enrich_lead(lead)
                        
                        if result:
                            batch_updates.append(result)
                            logger.info(
                                f"[FOUND] {lead['name']} -> {result['linkedin_url']} "
                                f"({result['confidence']}, {result['method']})"
                            )
                        else:
                            logger.debug(f"[NOT_FOUND] {lead['name']}")
                        
                        self.stats['processed'] += 1
                        
                        # Periodic batch save
                        if len(batch_updates) >= 10:
                            self.db.batch_update(batch_updates)
                            batch_updates = []
                        
                        # Progress logging
                        if self.stats['processed'] % 25 == 0:
                            self._log_progress()
                            
                    except Exception as e:
                        logger.error(f"Error processing lead {lead.get('id')}: {e}")
                        self.stats['errors'] += 1
                
                # Check if we've hit the limit
                if limit and self.stats['processed'] >= limit:
                    break
                
                offset += self.config.BATCH_SIZE
            
            # Final batch save
            if batch_updates:
                self.db.batch_update(batch_updates)
            
            self._log_final_stats()
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Saving pending updates...")
            if batch_updates:
                self.db.batch_update(batch_updates)
            self._log_final_stats()
        finally:
            self.db.close()
    
    def _log_progress(self):
        """Log current progress"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"Progress: {self.stats['processed']} processed | "
            f"{self.stats['found']} found | "
            f"{self.stats['not_found']} not found | "
            f"{rate:.1f} leads/sec"
        )
    
    def _log_final_stats(self):
        """Log final statistics"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        logger.info("=" * 70)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Duration: {elapsed / 60:.1f} minutes")
        logger.info(f"Processed: {self.stats['processed']}")
        logger.info(f"Found: {self.stats['found']}")
        logger.info(f"Not Found: {self.stats['not_found']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Skipped (no name): {self.stats['skipped']}")
        logger.info("-" * 40)
        logger.info("Found by Strategy:")
        for i in range(1, 6):
            count = self.stats['found_by_strategy'][i]
            logger.info(f"  Strategy {i}: {count}")
        logger.info("-" * 40)
        
        if self.stats['processed'] > 0:
            success_rate = (self.stats['found'] / self.stats['processed']) * 100
            logger.info(f"Success Rate: {success_rate:.1f}%")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enrich leads with LinkedIn URLs')
    parser.add_argument('--limit', type=int, help='Maximum leads to process')
    parser.add_argument('--offset', type=int, default=0, help='Starting offset')
    parser.add_argument('--no-fallback', action='store_true', 
                       help='Disable fallback custom search (strategy 5)')
    parser.add_argument('--batch-size', type=int, help='Batch size for processing')
    args = parser.parse_args()
    
    config = Config()
    
    if args.no_fallback:
        config.USE_FALLBACK_SEARCH = False
    if args.batch_size:
        config.BATCH_SIZE = args.batch_size
    
    logger.info("=" * 60)
    logger.info("LinkedIn Lead Enrichment")
    logger.info("=" * 60)
    logger.info(f"API: {config.SCRAPER_API_URL}")
    logger.info(f"Database: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    logger.info(f"Batch size: {config.BATCH_SIZE}")
    logger.info(f"Fallback search: {'enabled' if config.USE_FALLBACK_SEARCH else 'disabled'}")
    logger.info("=" * 60)
    
    enricher = LeadEnricher(config)
    enricher.run(limit=args.limit, start_offset=args.offset)


if __name__ == "__main__":
    main()
