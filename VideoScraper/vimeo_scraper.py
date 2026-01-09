import time
import re
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs, unquote, quote_plus, urlencode
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yt_dlp
from bs4 import BeautifulSoup
import pandas as pd
from config import LoggerConfig
import itertools
import random
from urllib import request

class VimeoScraper():
    def __init__(self, rate_limit_delay: float = 30.0, to_scrape: str = 'vimeo.com', cookies_from_browser: Optional[tuple] = ('Firefox', ), use_selenium: bool = False):
        """
        cookies_from_browser examples:
          None
          ("chrome",)
          ("chrome", "Default")
          ("chrome", "Profile 1")
        use_selenium: Use Selenium with real browser to avoid detection (slower but more reliable)
        """
        self.rate_limit_delay = rate_limit_delay
        self.cookies_from_browser = cookies_from_browser
        self.use_selenium = use_selenium
        self.logger = LoggerConfig.setup_logger(__name__)
        self.baseurl = 'https://lite.duckduckgo.com/lite/'
        self.to_scrape = to_scrape
        self.csv_file = 'data/Scraping_Part1_keywords_extended.csv'
        
        self.stats = {
            "queries_processed": 0,
            "total_videos_found": 0,
            "youtube_videos": 0,
            "vimeo_videos": 0,
            "errors": 0,
        }

        self.start_urls = []

        query_terms = pd.read_csv(self.csv_file)
        query_terms.fillna('', inplace=True)
        terms_emotion = set(query_terms.Emotion.unique())
        terms_subject = set(query_terms.Subject.unique())
        terms_setting = set(query_terms.Setting.unique())
        for s in [terms_emotion, terms_subject, terms_setting]:
            if '' in s:
                s.remove('')

        all_queries = list(itertools.product(terms_emotion, terms_setting, terms_subject))
        random.shuffle(all_queries)

        for p in all_queries:
            emo, setting, subj = p
            query = f"{emo} {subj} {setting}".strip()
            url_query = f"site:{self.to_scrape} {query}"
            self.start_urls.append(url_query)

        self.logger.info(f"Initialized VimeoScraper with {len(self.start_urls)} queries")

    def _get_session(self):
        """Create a session with retry strategy and connection pooling"""
        session = requests.Session()
        
        # Retry strategy voor tijdelijke fouten
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,  # Wait 2, 4, 8 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session

    def _get_random_headers(self):
        """Generate random but realistic headers to avoid detection"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        accept_languages = [
            'en-US,en;q=0.9',
            'en-GB,en;q=0.9',
            'en-US,en;q=0.9,nl;q=0.8',
            'en-US,en;q=0.8',
        ]
        
        return {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': random.choice(accept_languages),
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }

    def _search_with_selenium(self, query_part, max_results=5):
        """Use Selenium for browser automation to avoid CAPTCHA"""
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.firefox.options import Options
        
        videos = []
        
        try:
            # Setup Firefox with options to look like real user
            options = Options()
            # Don't use headless mode - it's easier to detect
            # options.add_argument('--headless')
            options.set_preference("general.useragent.override", 
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0")
            
            driver = webdriver.Firefox(options=options)
            
            try:
                # Go to DuckDuckGo Lite (simpler, less likely to trigger CAPTCHA)
                driver.get('https://lite.duckduckgo.com/lite/')
                time.sleep(random.uniform(2, 4))
                
                # Find search box and enter query
                search_box = driver.find_element(By.NAME, 'q')
                
                # Type like a human (character by character with delays)
                for char in query_part:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.05, 0.15))
                
                time.sleep(random.uniform(0.5, 1.5))
                
                # Submit search (find the submit button)
                submit_button = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"]')
                submit_button.click()
                
                # Wait for results to load
                time.sleep(random.uniform(3, 5))
                
                # DuckDuckGo Lite has simpler HTML structure
                # Results are in table rows with links
                result_links = driver.find_elements(By.CSS_SELECTOR, 'a.result-link')
                
                # If that doesn't work, try finding all links in the results area
                if not result_links:
                    result_links = driver.find_elements(By.XPATH, '//td[@class="result-snippet"]//a')
                
                # Fallback: find all links and filter for vimeo
                if not result_links:
                    all_links = driver.find_elements(By.TAG_NAME, 'a')
                    result_links = [link for link in all_links if link.get_attribute('href')]
                
                videos_found = 0
                for element in result_links:
                    if videos_found >= max_results:
                        break
                    
                    try:
                        href = element.get_attribute('href')
                        
                        # Skip if not a real link
                        if not href or href.startswith('javascript:') or href == '#':
                            continue
                        
                        # Check if it's a Vimeo link
                        if href and 'vimeo.com' in href:
                            title = element.text or 'No title'
                            
                            videos.append({
                                'url': href,
                                'title': title,
                                'query': query_part,
                                'search_position': videos_found + 1,
                                'scraped_at': pd.Timestamp.now()
                            })
                            videos_found += 1
                            self.logger.info(f"  Found Vimeo video {videos_found}: {href}")
                    except Exception as e:
                        self.logger.debug(f"Error extracting link: {e}")
                        continue
                
                if videos_found == 0:
                    # Log the page source for debugging
                    self.logger.warning("No Vimeo videos found. Checking page structure...")
                    # Optionally save HTML for debugging
                    # with open('debug_page.html', 'w', encoding='utf-8') as f:
                    #     f.write(driver.page_source)
                
            finally:
                driver.quit()
                
        except Exception as e:
            self.logger.error(f"Selenium error: {e}")
        
        return videos

    def search(self, max_results=5):
        """
        Goes through the list of URLs and takes the first max_results vimeo links of each search
        With advanced anti-detection measures
        """
        all_videos = []
        
        if self.use_selenium:
            self.logger.info("Using Selenium mode (slower but avoids CAPTCHA)")
            
            for idx, query_part in enumerate(self.start_urls):
                self.logger.info(f"Processing query {idx+1}/{len(self.start_urls)}: {query_part}")
                
                # Longer pause each 5 queries
                if idx > 0 and idx % 5 == 0:
                    extra_delay = random.uniform(60, 120)
                    self.logger.info(f"Taking extended break: {extra_delay:.1f} seconds")
                    time.sleep(extra_delay)
                
                videos = self._search_with_selenium(query_part, max_results)
                all_videos.extend(videos)
                
                self.stats['queries_processed'] += 1
                self.stats['vimeo_videos'] += len(videos)
                self.stats['total_videos_found'] += len(videos)
                
                # Longer delay between queries
                delay = random.uniform(self.rate_limit_delay, self.rate_limit_delay * 1.5)
                self.logger.info(f"Waiting {delay:.1f} seconds before next query...")
                time.sleep(delay)
                
                # Backup every 20 queries
                if len(all_videos) > 0 and idx % 20 == 0 and idx > 0:
                    df_temp = pd.DataFrame(all_videos)
                    df_temp.to_csv('data/vimeo_videos_backup.csv', index=False)
                    self.logger.info(f"Backup saved: {len(all_videos)} videos")
        else:
            self.logger.error("Can only search with Selenium!!")
    