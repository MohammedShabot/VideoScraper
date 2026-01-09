from yt_dlp import YoutubeDL
import csv
from tools import read_csv
from itertools import cycle
import time

class VideoAPI:
    def __init__(self):
        # Example: 'ytsearch5:cute dogs'
        self.baseurl = "ytsearch"

        self.search_prefixes = {
            'youtube':'ytsearch',
            'vimeo': 'vimeo',

        }

        # Options passed to YoutubeDL
        self.ydl_opts = {
            "ignoreerrors": True,
            "verbose": True,
            "writeautomaticsub": True,
            "sub_langs": ["en", "en-US", "en-us"],
            "match_filter": lambda i: ("skip" if i.get("duration", 0) > 180 else None),
        }

    @staticmethod
    def _clean_text(x):
        """Make text CSV-safe and avoid newline explosions."""
        if x is None:
            return ""
        return str(x).replace("\r", " ").replace("\n", " ").strip()


    def scrape(self, keywordsFile: str, topResults: int, output_csv: str = "scraped_videos.csv", proxyList=None):
        proxyList = proxyList or []
        proxy_cycle = cycle(proxyList) if proxyList else None

        shuffled_queries = read_csv(keywordsFile)
        fieldnames = ["Query", "Title", "VideoId", "URL", "Channel", "Duration", "Description"]

        with open(output_csv, "w", newline="", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for emotion, setting, subject in shuffled_queries:
                query = f"{emotion} {subject} {setting}"
                search_query = f"{self.baseurl}{topResults}:{query}"

                # Rotate proxy for this query (if provided)
                proxy = next(proxy_cycle) if proxy_cycle else None

                # Build per-request options (important: new YoutubeDL instance)
                per_request_opts = dict(self.ydl_opts)
                if proxy:
                    per_request_opts["proxy"] = proxy

                # Retry with different proxies on hard failure (recommended)
                max_attempts = min(5, len(proxyList)) if proxyList else 1
                last_err = None

                for attempt in range(1, max_attempts + 1):
                    if proxy_cycle and attempt > 1:
                        proxy = next(proxy_cycle)
                        per_request_opts["proxy"] = proxy

                    try:
                        with YoutubeDL(per_request_opts) as ydl:
                            info = ydl.extract_info(search_query, download=False)

                        entries = (info or {}).get("entries") or []
                        for entry in entries:
                            if not entry:
                                continue

                            video_id = entry.get("id")
                            url = (
                                entry.get("webpage_url")
                                or entry.get("url")
                                or (f"https://www.youtube.com/watch?v={video_id}" if video_id else "")
                            )

                            data = {
                                "Query": self._clean_text(query),
                                "Title": self._clean_text(entry.get("title")),
                                "VideoId": self._clean_text(video_id),
                                "URL": self._clean_text(url),
                                "Channel": self._clean_text(entry.get("channel") or entry.get("uploader")),
                                "Duration": entry.get("duration") or 0,
                                "Description": self._clean_text(entry.get("description")),
                            }

                            print(data)
                            writer.writerow(data)

                        # success → break retry loop
                        break

                    except Exception as e:
                        last_err = e
                        # small backoff
                        time.sleep(1.5)

                else:
                    # all attempts failed
                    print(f"[WARN] Failed query after retries: {query}. Last error: {last_err}")


if __name__ == "__main__":
    videoscraper = VideoAPI()
    videoscraper.scrape(
        keywordsFile="Scraping_Part1_keywords_extended.csv",
        topResults=5,
        output_csv="scraped_videos.csv",
    )
