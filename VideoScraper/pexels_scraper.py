import aiohttp
from dotenv import load_dotenv
import os
import pandas as pd
import itertools
import random
from config import LoggerConfig
import time
import csv

class PexelsScraper:
    def __init__(self, csv_file: str = 'data/Scraping_Part1_keywords_extended.csv', 
                 output_path: str = 'data/results/pexels_videos_scraped.csv'):
        self.baseurl = "https://api.pexels.com/videos/search"
        load_dotenv()
        self.pexels_key = os.getenv("PEXELS_API_KEY")
        self.logger = LoggerConfig.setup_logger(__name__)
        self.csv_file = csv_file
        self.output_path = output_path

        self.queries = []
        self.fieldnames = ['query', 'url']

        self.video_query_links = []

        query_terms = pd.read_csv(self.csv_file)
        query_terms.fillna('', inplace=True)

        terms_emotion = set(query_terms.Emotion.unique())
        terms_subject = set(query_terms.Subject.unique())
        terms_setting = set(query_terms.Setting.unique())
        for s in [terms_emotion, terms_subject, terms_setting]:
            s.discard('')

        all_queries = list(itertools.product(terms_emotion, terms_setting, terms_subject))
        random.shuffle(all_queries)

        for emo, setting, subj in all_queries:
            self.queries.append(f"{emo} {subj} {setting}".strip())

    async def scrape_pexels(self, session: aiohttp.ClientSession, query: str, max_results: int = 10):
        self.logger.info(f"Scraping Pexels for query: {query}")

        params = {
            "query": query,
            "orientation": "landscape",
            "locale": "en-US",
            "per_page": max_results,
        }

        headers = {
            "Authorization": self.pexels_key
        }
        fixed_delay = 2
        random_delay = random.uniform(3, 6)
        final_delay = fixed_delay + random_delay

        time.sleep(final_delay)
        async with session.get(self.baseurl, params=params, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data

    async def run_scraper(self):
        async with aiohttp.ClientSession() as session:
            for query in self.queries:
                data = await self.scrape_pexels(session, query)
                scraped_video_links = data['videos']
                video_links = []
                for video_link in scraped_video_links:
                    video_links.append((video_link['url'], video_link['video_files'][0]['link']))
                self.video_query_links.append({query: video_links})
                

                print("------------------ THESE ARE THE VIDEO LINKS --------------------------")
                print(self.video_query_links)

                # ✅ Fixed Excel writing code
                excel_path = self.output_path.rsplit('.', 1)[0] + '.xlsx'

                rows = []
                # Extract direct video URLs from tuples (pexels_page_url, direct_video_url)
                for page_url, direct_url in video_links:
                    rows.append({"query": query, "url": direct_url})

                df = pd.DataFrame(rows, columns=self.fieldnames)

                if not os.path.exists(excel_path):
                    df.to_excel(excel_path, index=False)
                else:
                    # Read existing data and append new data
                    existing_df = pd.read_excel(excel_path)
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df.to_excel(excel_path, index=False)

                # Write to CSV file
                with open(self.output_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)