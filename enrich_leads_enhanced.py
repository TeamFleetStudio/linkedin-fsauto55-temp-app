"""
Enhanced LinkedIn Lead Enrichment with Multiple Search Providers
- Primary: Google Custom Search API (5 keys)
- Fallback: Yahoo, DuckDuckGo, Bing
- Safe batch processing for 100k+ records
"""

import os
import time
import logging
import json
from typing import Optional, Dict, List
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from dotenv import load_dotenv

from fallback_search import FallbackSearcher, search_with_fallback_strategies

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
    # Database
    DB_HOST: str = os.getenv('DB_HOST', 'localhost')
    DB_PORT: int = int(os.getenv('DB_PORT', 5432))
    DB_NAME: str = os.getenv('DB_NAME', 'enrich')
    DB_USER: str = os.getenv('DB_USER', 'admin')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', '')
    
    # Google Custom Search API keys (5 for fallback)
    GOOGLE_API_KEYS: List[str] = None
    GOOGLE_CSE_ID: str = os.getenv('GOOGLE_CSE_ID', '')
    
    # Processing settings
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', 50))
    REQUESTS_PER_SECOND: float = float(os.getenv('REQUESTS_PER_SECOND', 0.5))
    USE_FALLBACK_ENGINES: bool = os.getenv('USE_FALLBACK_ENGINES', 'true').lower() == 'true'
    
    def __post_init__(self):
        self.GOOGLE_API_KEYS = [
            os.getenv(f'GOOGLE_API_KEY_{i}', '') 
            for i in range(1, 6)
        ]
        self.GOOGLE_API_KEYS = [k for k in self.GOOGLE_API_KEYS if k]


class APIKeyManager:
    """Manages Google API keys with rotation"""
    
    def __init__(self, keys: List[str]):
        self.keys = keys
        self.current_idx = 0
        self.exhausted = set()
        self.request_counts = {i: 0 for i in range(len(keys))}
        
    def get_key(self) -> Optional[str]:
        if not self.keys or len(self.exhausted) >= len(self.keys):
            return None
        return self.keys[self.current_idx]
    
    def mark_exhausted(self):
        logger.warning(f"API key {self.current_idx + 1} exhausted "
                      f"(used {self.request_counts[self.current_idx]} requests)")
        self.exhausted.add(self.current_idx)
        self._rotate()
        
    def _rotate(self):
        for i in range(len(self.keys)):
            next_idx = (self.current_idx + 1 + i) % len(self.keys)
            if next_idx not in self.exhausted:
                self.current_idx = next_idx
                logger.info(f"Switched to API key {next_idx + 1}")
                return
                
    def increment_count(self):
        self.request_counts[self.current_idx] += 1
        
    def has_keys(self) -> bool:
        return len(self.exhausted) < len(self.keys)
    
    def get_stats(self) -> Dict:
        return {
            'total_keys': len(self.keys),
            'exhausted': len(self.exhausted),
            'request_counts': self.request_counts
        }


class DatabaseManager:
    """PostgreSQL database operations"""
    
    def __init__(self, config: Config):
        self.config = config
        self.conn = None
        
    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config.DB_HOST,
            port=self.config.DB_PORT,
            dbname=self.config.DB_NAME,
            user=self.config.DB_USER,
            password=self.config.DB_PASSWORD
        )
        self.conn.autocommit = False
        logger.info(f"Connected to database: {self.config.DB_NAME}")
        
    def close(self):
        if self.conn:
            self.conn.close()
            
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
    
    def update_linkedin_url(self, lead_id: str, linkedin_url: str, 
                           search_method: str = None):
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
            'linkedin_search_method': search_method
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
                    'linkedin_search_method': update.get('method', 'unknown')
                })
                cur.execute(query, (
                    update['linkedin_url'],
                    enrichment_meta,
                    update['id']
                ))
        self.conn.commit()
        logger.info(f"Batch updated {len(updates)} leads")


class GoogleSearcher:
    """Google Custom Search API client"""
    
    URL = "https://www.googleapis.com/customsearch/v1"
    
    def __init__(self, key_manager: APIKeyManager, cse_id: str, 
                 rate_limit: float = 1.0):
        self.key_manager = key_manager
        self.cse_id = cse_id
        self.min_interval = 1.0 / rate_limit
        self.last_request = 0
        
    def _wait_rate_limit(self):
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()
        
    def _build_query(self, name: str, company: str = None, 
                     location: str = None) -> str:
        parts = [f'"{name}"', 'site:linkedin.com/in']
        if company:
            parts.insert(1, company)
        if location:
            parts.insert(-1, location)
        return ' '.join(parts)
    
    def _extract_linkedin(self, results: Dict) -> Optional[str]:
        for item in results.get('items', []):
            link = item.get('link', '')
            if 'linkedin.com/in/' in link:
                # Clean URL
                return link.split('?')[0] if '?' in link else link
        return None
    
    def search(self, query: str) -> Optional[str]:
        """Execute search with current API key"""
        api_key = self.key_manager.get_key()
        if not api_key:
            return None
            
        self._wait_rate_limit()
        self.key_manager.increment_count()
        
        try:
            resp = requests.get(self.URL, params={
                'key': api_key,
                'cx': self.cse_id,
                'q': query,
                'num': 5
            }, timeout=30)
            
            if resp.status_code == 200:
                return self._extract_linkedin(resp.json())
            elif resp.status_code == 429 or 'quotaExceeded' in resp.text:
                self.key_manager.mark_exhausted()
                if self.key_manager.has_keys():
                    return self.search(query)  # Retry with new key
            else:
                logger.warning(f"Google API error: {resp.status_code}")
                
        except Exception as e:
            logger.error(f"Google search error: {e}")
            
        return None
    
    def search_with_strategies(self, lead: Dict) -> tuple[Optional[str], str]:
        """
        Search using multiple strategies
        Returns (linkedin_url, method_used)
        """
        name = lead.get('name', '').strip()
        if not name:
            return None, ''
            
        brand = lead.get('brand_name', '').strip()
        metadata = lead.get('metadata', {})
        
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except:
                metadata = {}
        
        business_name = metadata.get('business_name', '')
        city_state_zip = metadata.get('city_state_zip', '')
        
        # Parse location
        city, state, zip_code = '', '', ''
        if city_state_zip:
            parts = city_state_zip.split(',')
            city = parts[0].strip() if parts else ''
            if len(parts) > 1:
                state_zip = parts[1].strip().split()
                state = state_zip[0] if state_zip else ''
                zip_code = state_zip[1] if len(state_zip) > 1 else ''
        
        # Define search strategies
        strategies = [
            ('google_brand_location', brand, f"{city} {state}".strip()),
            ('google_business_location', business_name, f"{city} {zip_code}".strip()),
            ('google_brand_only', brand, None),
            ('google_business_only', business_name, None),
            ('google_name_location', None, f"{city} {state}".strip()),
        ]
        
        for method, company, location in strategies:
            if not self.key_manager.has_keys():
                return None, 'api_exhausted'
                
            # Skip empty strategies
            if not company and not location:
                continue
                
            query = self._build_query(name, company, location)
            logger.debug(f"Strategy {method}: {query}")
            
            result = self.search(query)
            if result:
                return result, method
                
            time.sleep(0.3)  # Brief pause between strategies
            
        return None, 'not_found'


class LeadEnricher:
    """Main enrichment orchestrator"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config)
        self.key_manager = APIKeyManager(config.GOOGLE_API_KEYS)
        self.google = GoogleSearcher(
            self.key_manager, 
            config.GOOGLE_CSE_ID,
            config.REQUESTS_PER_SECOND
        )
        self.fallback = FallbackSearcher(delay_between_requests=3.0) if config.USE_FALLBACK_ENGINES else None
        
        self.stats = {
            'processed': 0,
            'found_google': 0,
            'found_fallback': 0,
            'not_found': 0,
            'errors': 0,
            'start_time': None
        }
        
    def enrich_lead(self, lead: Dict) -> Optional[Dict]:
        """Enrich single lead with LinkedIn URL"""
        try:
            # Try Google Custom Search first
            linkedin_url, method = self.google.search_with_strategies(lead)
            
            if linkedin_url:
                self.stats['found_google'] += 1
                return {
                    'id': lead['id'],
                    'linkedin_url': linkedin_url,
                    'method': method
                }
            
            # Fallback to free search engines
            if self.fallback and self.config.USE_FALLBACK_ENGINES:
                linkedin_url = search_with_fallback_strategies(self.fallback, lead)
                if linkedin_url:
                    self.stats['found_fallback'] += 1
                    return {
                        'id': lead['id'],
                        'linkedin_url': linkedin_url,
                        'method': 'fallback_search'
                    }
            
            self.stats['not_found'] += 1
            return None
            
        except Exception as e:
            logger.error(f"Error enriching lead {lead.get('id')}: {e}")
            self.stats['errors'] += 1
            return None
        finally:
            self.stats['processed'] += 1
    
    def run(self, limit: Optional[int] = None, start_offset: int = 0):
        """Run enrichment process"""
        self.stats['start_time'] = datetime.now()
        
        try:
            self.db.connect()
            
            total = self.db.count_leads_to_enrich()
            logger.info(f"Total leads to enrich: {total}")
            
            if limit:
                total = min(total, limit)
                logger.info(f"Limiting to {limit} leads")
            
            offset = start_offset
            batch_updates = []
            
            while offset < total + start_offset:
                # Check API availability
                if not self.key_manager.has_keys() and not self.config.USE_FALLBACK_ENGINES:
                    logger.error("All API keys exhausted and fallback disabled")
                    break
                
                leads = self.db.get_leads_to_enrich(self.config.BATCH_SIZE, offset)
                if not leads:
                    break
                
                batch_num = (offset - start_offset) // self.config.BATCH_SIZE + 1
                logger.info(f"Batch {batch_num}: Processing {len(leads)} leads "
                           f"(offset {offset})")
                
                for lead in leads:
                    result = self.enrich_lead(lead)
                    
                    if result:
                        batch_updates.append(result)
                        logger.info(f"[FOUND] {lead['name']} -> {result['linkedin_url']} "
                                   f"({result['method']})")
                    else:
                        logger.debug(f"[NOT_FOUND] {lead['name']}")
                    
                    # Periodic batch save
                    if len(batch_updates) >= 10:
                        self.db.batch_update(batch_updates)
                        batch_updates = []
                    
                    # Progress logging
                    if self.stats['processed'] % 50 == 0:
                        self._log_progress()
                
                offset += self.config.BATCH_SIZE
            
            # Final batch save
            if batch_updates:
                self.db.batch_update(batch_updates)
            
            self._log_final_stats()
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Saving pending updates...")
            if batch_updates:
                self.db.batch_update(batch_updates)
        finally:
            self.db.close()
    
    def _log_progress(self):
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
        
        found = self.stats['found_google'] + self.stats['found_fallback']
        logger.info(
            f"Progress: {self.stats['processed']} processed | "
            f"{found} found ({self.stats['found_google']} Google, "
            f"{self.stats['found_fallback']} fallback) | "
            f"{rate:.1f} leads/sec"
        )
        
        # Log API key status
        key_stats = self.key_manager.get_stats()
        logger.info(f"API Keys: {key_stats['total_keys'] - key_stats['exhausted']}"
                   f"/{key_stats['total_keys']} available")
    
    def _log_final_stats(self):
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        found = self.stats['found_google'] + self.stats['found_fallback']
        
        logger.info("=" * 70)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Duration: {elapsed / 60:.1f} minutes")
        logger.info(f"Processed: {self.stats['processed']}")
        logger.info(f"Found (Google): {self.stats['found_google']}")
        logger.info(f"Found (Fallback): {self.stats['found_fallback']}")
        logger.info(f"Not Found: {self.stats['not_found']}")
        logger.info(f"Errors: {self.stats['errors']}")
        
        if self.stats['processed'] > 0:
            success_rate = (found / self.stats['processed']) * 100
            logger.info(f"Success Rate: {success_rate:.1f}%")
        
        logger.info(f"API Key Stats: {self.key_manager.get_stats()}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enrich leads with LinkedIn URLs')
    parser.add_argument('--limit', type=int, help='Maximum leads to process')
    parser.add_argument('--offset', type=int, default=0, help='Starting offset')
    parser.add_argument('--no-fallback', action='store_true', 
                       help='Disable fallback search engines')
    args = parser.parse_args()
    
    config = Config()
    
    if args.no_fallback:
        config.USE_FALLBACK_ENGINES = False
    
    # Validate config
    if not config.GOOGLE_API_KEYS:
        logger.warning("No Google API keys configured. Using fallback engines only.")
        if not config.USE_FALLBACK_ENGINES:
            logger.error("No search methods available!")
            return
    
    if config.GOOGLE_API_KEYS and not config.GOOGLE_CSE_ID:
        logger.error("GOOGLE_CSE_ID required when using Google API")
        return
    
    logger.info(f"Starting enrichment with {len(config.GOOGLE_API_KEYS)} Google API keys")
    logger.info(f"Fallback engines: {'enabled' if config.USE_FALLBACK_ENGINES else 'disabled'}")
    
    enricher = LeadEnricher(config)
    enricher.run(limit=args.limit, start_offset=args.offset)


if __name__ == "__main__":
    main()
