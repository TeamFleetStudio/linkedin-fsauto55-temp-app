"""
Fallback LinkedIn Search using Yahoo/DuckDuckGo
Used when Google Custom Search API quota is exhausted
"""

import re
import time
import urllib.parse
import logging
from typing import Optional, List, Dict
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class FallbackSearcher:
    """
    Fallback search engines when Google Custom Search API is exhausted
    Uses Yahoo and DuckDuckGo as alternatives
    """
    
    def __init__(self, delay_between_requests: float = 3.0):
        self.session = requests.Session()
        self.delay = delay_between_requests
        self.last_request_time = 0
        
    def _rate_limit(self):
        """Enforce rate limiting to avoid blocks"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()
        
    def _extract_linkedin_url(self, text: str) -> Optional[str]:
        """Extract LinkedIn /in/ profile URL from text"""
        # Decode URL-encoded text
        decoded = urllib.parse.unquote(text)
        
        # Find LinkedIn profile URL
        match = re.search(
            r'(https?://(?:www\.)?linkedin\.com/in/[A-Za-z0-9_-]+)',
            decoded
        )
        
        if match:
            url = match.group(1)
            # Normalize URL
            url = url.replace("http://", "https://")
            if "www." not in url:
                url = url.replace("linkedin.com", "www.linkedin.com")
            return url
        return None
    
    def search_yahoo(self, name: str, company: str = None, 
                     location: str = None) -> Optional[str]:
        """Search Yahoo for LinkedIn profile"""
        query_parts = [name]
        if company:
            query_parts.append(company)
        if location:
            query_parts.append(location)
        query_parts.append("linkedin")
        
        query = " ".join(query_parts)
        encoded = urllib.parse.quote(query)
        url = f"https://search.yahoo.com/search?p={encoded}"
        
        logger.debug(f"Yahoo search: {query}")
        self._rate_limit()
        
        try:
            resp = self.session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            for a_tag in soup.select("a"):
                href = a_tag.get("href", "")
                if "linkedin" in href.lower():
                    linkedin_url = self._extract_linkedin_url(href)
                    if linkedin_url:
                        logger.info(f"Yahoo found: {linkedin_url}")
                        return linkedin_url
                        
            return None
            
        except Exception as e:
            logger.error(f"Yahoo search error: {e}")
            return None
    
    def search_duckduckgo(self, name: str, company: str = None,
                          location: str = None) -> Optional[str]:
        """Search DuckDuckGo for LinkedIn profile"""
        query_parts = [name]
        if company:
            query_parts.append(company)
        if location:
            query_parts.append(location)
        query_parts.append("site:linkedin.com/in")
        
        query = " ".join(query_parts)
        encoded = urllib.parse.quote(query)
        url = f"https://html.duckduckgo.com/html/?q={encoded}"
        
        logger.debug(f"DuckDuckGo search: {query}")
        self._rate_limit()
        
        try:
            resp = self.session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # DuckDuckGo results are in .result__url class
            for result in soup.select(".result__url"):
                href = result.get("href", "")
                if not href:
                    href = result.text
                    
                if "linkedin.com/in" in href.lower():
                    linkedin_url = self._extract_linkedin_url(href)
                    if linkedin_url:
                        logger.info(f"DuckDuckGo found: {linkedin_url}")
                        return linkedin_url
            
            # Also check regular links
            for a_tag in soup.select("a.result__a"):
                href = a_tag.get("href", "")
                if "linkedin.com/in" in href.lower():
                    linkedin_url = self._extract_linkedin_url(href)
                    if linkedin_url:
                        return linkedin_url
                        
            return None
            
        except Exception as e:
            logger.error(f"DuckDuckGo search error: {e}")
            return None
    
    def search_bing(self, name: str, company: str = None,
                    location: str = None) -> Optional[str]:
        """Search Bing for LinkedIn profile"""
        query_parts = [name]
        if company:
            query_parts.append(company)
        if location:
            query_parts.append(location)
        query_parts.append("site:linkedin.com/in")
        
        query = " ".join(query_parts)
        encoded = urllib.parse.quote(query)
        url = f"https://www.bing.com/search?q={encoded}"
        
        logger.debug(f"Bing search: {query}")
        self._rate_limit()
        
        try:
            resp = self.session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            for a_tag in soup.select("a"):
                href = a_tag.get("href", "")
                if "linkedin.com/in" in href.lower():
                    linkedin_url = self._extract_linkedin_url(href)
                    if linkedin_url:
                        logger.info(f"Bing found: {linkedin_url}")
                        return linkedin_url
                        
            return None
            
        except Exception as e:
            logger.error(f"Bing search error: {e}")
            return None
    
    def search_all_engines(self, name: str, company: str = None,
                           location: str = None) -> Optional[str]:
        """
        Search all fallback engines in sequence
        Returns first successful result
        """
        # Try Yahoo first
        result = self.search_yahoo(name, company, location)
        if result:
            return result
            
        # Try DuckDuckGo
        result = self.search_duckduckgo(name, company, location)
        if result:
            return result
            
        # Try Bing
        result = self.search_bing(name, company, location)
        if result:
            return result
            
        return None


def search_with_fallback_strategies(searcher: FallbackSearcher, 
                                    lead: Dict) -> Optional[str]:
    """
    Search for LinkedIn profile using fallback strategies
    Similar to main enrich_leads.py but using free search engines
    """
    import json
    
    name = lead.get('name', '').strip()
    if not name:
        return None
        
    brand_name = lead.get('brand_name', '').strip()
    metadata = lead.get('metadata', {})
    
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    
    city_state_zip = metadata.get('city_state_zip', '')
    business_name = metadata.get('business_name', '')
    
    # Parse location
    city = ''
    state = ''
    if city_state_zip:
        parts = city_state_zip.split(',')
        if parts:
            city = parts[0].strip()
        if len(parts) > 1:
            state_parts = parts[1].strip().split()
            if state_parts:
                state = state_parts[0]
    
    location = f"{city} {state}".strip() if city else None
    
    # Strategy 1: name + brand_name + location
    if brand_name:
        result = searcher.search_all_engines(name, brand_name, location)
        if result:
            return result
    
    # Strategy 2: name + business_name + location
    if business_name and business_name != brand_name:
        result = searcher.search_all_engines(name, business_name, location)
        if result:
            return result
    
    # Strategy 3: name + company (any) only
    company = brand_name or business_name
    if company:
        result = searcher.search_all_engines(name, company, None)
        if result:
            return result
    
    # Strategy 4: name + location only
    if location:
        result = searcher.search_all_engines(name, None, location)
        if result:
            return result
    
    # Strategy 5: name only
    result = searcher.search_all_engines(name, None, None)
    return result


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    searcher = FallbackSearcher(delay_between_requests=3.0)
    
    # Test with sample lead
    test_lead = {
        'name': 'John Smith',
        'brand_name': 'ABC Corp',
        'metadata': {
            'business_name': 'ABC Corporation',
            'city_state_zip': 'Boston, MA 02101'
        }
    }
    
    result = search_with_fallback_strategies(searcher, test_lead)
    print(f"Result: {result}")
