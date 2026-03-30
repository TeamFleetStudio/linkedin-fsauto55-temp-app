"""
LinkedIn Lead Enrichment Script
- Fetches leads from PostgreSQL database
- Uses Google Custom Search API with 5 fallback strategies
- Skips leads that already have linkedin_url
- Batch processing for 100k+ records
"""

import os
import time
import logging
import json
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('enrich_leads.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
@dataclass
class Config:
    # Database
    DB_HOST: str = os.getenv('DB_HOST', 'localhost')
    DB_PORT: int = int(os.getenv('DB_PORT', 5432))
    DB_NAME: str = os.getenv('DB_NAME', 'enrich')
    DB_USER: str = os.getenv('DB_USER', 'admin')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', '')
    
    # Google Custom Search API - 5 API keys for fallback
    GOOGLE_API_KEYS: List[str] = None
    GOOGLE_CSE_ID: str = os.getenv('GOOGLE_CSE_ID', '')
    
    # Rate limiting
    REQUESTS_PER_SECOND: float = 1.0
    BATCH_SIZE: int = 100
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5
    
    def __post_init__(self):
        # Load multiple API keys for fallback
        self.GOOGLE_API_KEYS = [
            os.getenv('GOOGLE_API_KEY_1', ''),
            os.getenv('GOOGLE_API_KEY_2', ''),
            os.getenv('GOOGLE_API_KEY_3', ''),
            os.getenv('GOOGLE_API_KEY_4', ''),
            os.getenv('GOOGLE_API_KEY_5', ''),
        ]
        # Filter out empty keys
        self.GOOGLE_API_KEYS = [k for k in self.GOOGLE_API_KEYS if k]


class APIKeyManager:
    """Manages multiple API keys with rotation on quota exhaustion"""
    
    def __init__(self, api_keys: List[str]):
        self.api_keys = api_keys
        self.current_index = 0
        self.exhausted_keys = set()
        
    def get_current_key(self) -> Optional[str]:
        """Get current active API key"""
        available_keys = [k for i, k in enumerate(self.api_keys) 
                         if i not in self.exhausted_keys]
        if not available_keys:
            return None
        return self.api_keys[self.current_index]
    
    def mark_exhausted(self):
        """Mark current key as exhausted and rotate to next"""
        self.exhausted_keys.add(self.current_index)
        logger.warning(f"API key {self.current_index + 1} exhausted, rotating...")
        self._rotate()
        
    def _rotate(self):
        """Rotate to next available key"""
        for i in range(len(self.api_keys)):
            next_index = (self.current_index + 1 + i) % len(self.api_keys)
            if next_index not in self.exhausted_keys:
                self.current_index = next_index
                logger.info(f"Switched to API key {self.current_index + 1}")
                return
        logger.error("All API keys exhausted!")
        
    def has_available_keys(self) -> bool:
        """Check if any keys are still available"""
        return len(self.exhausted_keys) < len(self.api_keys)
    
    def reset(self):
        """Reset all keys (for new day quota reset)"""
        self.exhausted_keys.clear()
        self.current_index = 0


class DatabaseManager:
    """Handles database connections and queries"""
    
    def __init__(self, config: Config):
        self.config = config
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        self.conn = psycopg2.connect(
            host=self.config.DB_HOST,
            port=self.config.DB_PORT,
            dbname=self.config.DB_NAME,
            user=self.config.DB_USER,
            password=self.config.DB_PASSWORD
        )
        logger.info("Database connection established")
        
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
            
    def get_leads_without_linkedin(self, batch_size: int, offset: int = 0) -> List[Dict]:
        """Fetch leads that don't have linkedin_url"""
        query = """
            SELECT id, name, brand_name, metadata, email
            FROM public.leads
            WHERE linkedin_url IS NULL OR linkedin_url = ''
            ORDER BY created_at ASC
            LIMIT %s OFFSET %s
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (batch_size, offset))
            return cur.fetchall()
    
    def get_total_leads_without_linkedin(self) -> int:
        """Get count of leads without linkedin_url"""
        query = """
            SELECT COUNT(*) as count
            FROM public.leads
            WHERE linkedin_url IS NULL OR linkedin_url = ''
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result['count'] if result else 0
    
    def update_linkedin_url(self, lead_id: str, linkedin_url: str):
        """Update linkedin_url for a specific lead"""
        query = """
            UPDATE public.leads
            SET linkedin_url = %s, updated_at = NOW()
            WHERE id = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (linkedin_url, lead_id))
            self.conn.commit()
            
    def batch_update_linkedin_urls(self, updates: List[Dict]):
        """Batch update linkedin_urls for multiple leads"""
        if not updates:
            return
            
        query = """
            UPDATE public.leads
            SET linkedin_url = %s, updated_at = NOW()
            WHERE id = %s
        """
        with self.conn.cursor() as cur:
            for update in updates:
                cur.execute(query, (update['linkedin_url'], update['id']))
            self.conn.commit()
        logger.info(f"Batch updated {len(updates)} leads")


class LinkedInSearcher:
    """Searches for LinkedIn profiles using Google Custom Search API"""
    
    GOOGLE_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"
    
    def __init__(self, config: Config):
        self.config = config
        self.key_manager = APIKeyManager(config.GOOGLE_API_KEYS)
        self.last_request_time = 0
        
    def _rate_limit(self):
        """Enforce rate limiting"""
        elapsed = time.time() - self.last_request_time
        min_interval = 1.0 / self.config.REQUESTS_PER_SECOND
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.time()
        
    def _extract_linkedin_url(self, search_results: Dict) -> Optional[str]:
        """Extract LinkedIn profile URL from search results"""
        items = search_results.get('items', [])
        for item in items:
            link = item.get('link', '')
            # Match LinkedIn profile URLs
            if 'linkedin.com/in/' in link:
                # Clean up the URL
                if '?' in link:
                    link = link.split('?')[0]
                return link
        return None
    
    def _build_search_query(self, name: str, company: str = None, 
                           location: str = None) -> str:
        """Build search query for LinkedIn profile"""
        # Base query with name and LinkedIn site restriction
        query_parts = [f'"{name}"', 'site:linkedin.com/in']
        
        if company:
            query_parts.insert(1, company)
        if location:
            query_parts.insert(2, location)
            
        return ' '.join(query_parts)
    
    def _make_search_request(self, query: str) -> Optional[Dict]:
        """Make a single search request to Google Custom Search API"""
        api_key = self.key_manager.get_current_key()
        if not api_key:
            logger.error("No available API keys")
            return None
            
        self._rate_limit()
        
        params = {
            'key': api_key,
            'cx': self.config.GOOGLE_CSE_ID,
            'q': query,
            'num': 5  # Get top 5 results
        }
        
        try:
            response = requests.get(
                self.GOOGLE_SEARCH_URL,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429 or 'quotaExceeded' in response.text:
                # Quota exceeded, rotate to next key
                self.key_manager.mark_exhausted()
                if self.key_manager.has_available_keys():
                    return self._make_search_request(query)  # Retry with new key
                return None
            else:
                logger.error(f"Search API error: {response.status_code} - {response.text}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            return None
    
    def search_linkedin(self, lead: Dict) -> Optional[str]:
        """
        Search for LinkedIn profile with multiple fallback strategies
        
        Strategies:
        1. name + brand_name (as company) + city from city_state_zip
        2. name + business_name (from metadata) + city + zip
        3. name + brand_name only
        4. name + business_name only
        5. name + location only
        """
        name = lead.get('name', '').strip()
        if not name:
            return None
            
        brand_name = lead.get('brand_name', '').strip()
        metadata = lead.get('metadata', {})
        
        # Handle metadata if it's a string
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        
        # Extract location info
        city_state_zip = metadata.get('city_state_zip', '')
        business_name = metadata.get('business_name', '')
        
        # Parse city and zip from city_state_zip (format: "Framingham, MA 01702")
        city = ''
        state = ''
        zip_code = ''
        if city_state_zip:
            parts = city_state_zip.split(',')
            if parts:
                city = parts[0].strip()
            if len(parts) > 1:
                state_zip = parts[1].strip().split()
                if state_zip:
                    state = state_zip[0]
                if len(state_zip) > 1:
                    zip_code = state_zip[1]
        
        # Define search strategies with fallbacks
        search_strategies = [
            # Strategy 1: name + brand_name + city + state_zip
            {
                'name': name,
                'company': brand_name,
                'location': f"{city} {state}" if city else None
            },
            # Strategy 2: name + business_name + city + zip
            {
                'name': name,
                'company': business_name,
                'location': f"{city} {zip_code}" if city else None
            },
            # Strategy 3: name + brand_name only
            {
                'name': name,
                'company': brand_name,
                'location': None
            },
            # Strategy 4: name + business_name only
            {
                'name': name,
                'company': business_name,
                'location': None
            },
            # Strategy 5: name + location only
            {
                'name': name,
                'company': None,
                'location': f"{city} {state}" if city else None
            }
        ]
        
        # Try each strategy until we find a result
        for i, strategy in enumerate(search_strategies, 1):
            if not self.key_manager.has_available_keys():
                logger.error("All API keys exhausted, stopping search")
                return None
                
            # Skip strategies with no useful data
            if not strategy['company'] and not strategy['location']:
                if i > 1:  # Already tried name-only in a previous strategy
                    continue
                    
            query = self._build_search_query(
                name=strategy['name'],
                company=strategy['company'],
                location=strategy['location']
            )
            
            logger.debug(f"Strategy {i}: {query}")
            
            results = self._make_search_request(query)
            if results:
                linkedin_url = self._extract_linkedin_url(results)
                if linkedin_url:
                    logger.info(f"Found LinkedIn URL using strategy {i}: {linkedin_url}")
                    return linkedin_url
                    
            # Small delay between fallback attempts
            time.sleep(0.5)
        
        logger.info(f"No LinkedIn URL found for: {name}")
        return None


class LeadEnricher:
    """Main class to orchestrate lead enrichment"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config)
        self.searcher = LinkedInSearcher(config)
        self.stats = {
            'processed': 0,
            'found': 0,
            'not_found': 0,
            'errors': 0,
            'skipped': 0
        }
        
    def run(self, limit: Optional[int] = None):
        """Run the enrichment process"""
        try:
            self.db.connect()
            
            total_leads = self.db.get_total_leads_without_linkedin()
            logger.info(f"Total leads without LinkedIn URL: {total_leads}")
            
            if limit:
                total_leads = min(total_leads, limit)
                logger.info(f"Processing limited to {limit} leads")
            
            offset = 0
            batch_updates = []
            
            while offset < total_leads:
                # Check if we still have API keys
                if not self.searcher.key_manager.has_available_keys():
                    logger.error("All API keys exhausted, stopping enrichment")
                    break
                
                # Fetch batch of leads
                leads = self.db.get_leads_without_linkedin(
                    batch_size=self.config.BATCH_SIZE,
                    offset=offset
                )
                
                if not leads:
                    break
                    
                logger.info(f"Processing batch {offset // self.config.BATCH_SIZE + 1} "
                           f"({len(leads)} leads)")
                
                for lead in leads:
                    try:
                        # Search for LinkedIn URL
                        linkedin_url = self.searcher.search_linkedin(lead)
                        
                        if linkedin_url:
                            batch_updates.append({
                                'id': lead['id'],
                                'linkedin_url': linkedin_url
                            })
                            self.stats['found'] += 1
                            logger.info(f"[FOUND] {lead['name']} -> {linkedin_url}")
                        else:
                            self.stats['not_found'] += 1
                            logger.info(f"[NOT_FOUND] {lead['name']}")
                            
                        self.stats['processed'] += 1
                        
                        # Batch update every 10 records
                        if len(batch_updates) >= 10:
                            self.db.batch_update_linkedin_urls(batch_updates)
                            batch_updates = []
                            
                    except Exception as e:
                        logger.error(f"Error processing lead {lead['id']}: {e}")
                        self.stats['errors'] += 1
                        
                    # Progress logging
                    if self.stats['processed'] % 100 == 0:
                        self._log_progress()
                        
                offset += self.config.BATCH_SIZE
                
            # Final batch update
            if batch_updates:
                self.db.batch_update_linkedin_urls(batch_updates)
                
            self._log_final_stats()
            
        finally:
            self.db.close()
            
    def _log_progress(self):
        """Log current progress"""
        logger.info(
            f"Progress: {self.stats['processed']} processed, "
            f"{self.stats['found']} found, "
            f"{self.stats['not_found']} not found, "
            f"{self.stats['errors']} errors"
        )
        
    def _log_final_stats(self):
        """Log final statistics"""
        logger.info("=" * 60)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total Processed: {self.stats['processed']}")
        logger.info(f"LinkedIn Found: {self.stats['found']}")
        logger.info(f"Not Found: {self.stats['not_found']}")
        logger.info(f"Errors: {self.stats['errors']}")
        if self.stats['processed'] > 0:
            success_rate = (self.stats['found'] / self.stats['processed']) * 100
            logger.info(f"Success Rate: {success_rate:.2f}%")


def main():
    """Main entry point"""
    config = Config()
    
    # Validate configuration
    if not config.GOOGLE_API_KEYS:
        logger.error("No Google API keys configured. Please set GOOGLE_API_KEY_1 through GOOGLE_API_KEY_5")
        return
        
    if not config.GOOGLE_CSE_ID:
        logger.error("Google Custom Search Engine ID not configured. Please set GOOGLE_CSE_ID")
        return
    
    logger.info(f"Starting LinkedIn enrichment with {len(config.GOOGLE_API_KEYS)} API keys")
    
    enricher = LeadEnricher(config)
    
    # Run with optional limit for testing
    # enricher.run(limit=100)  # Uncomment to test with 100 leads
    enricher.run()


if __name__ == "__main__":
    main()
