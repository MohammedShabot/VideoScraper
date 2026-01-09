import json
import os
from datetime import datetime
from typing import List, Dict, Optional

from query_generator import QueryGenerator
from VideoScraper.youtube_scraper import VideoScraper
from config import LoggerConfig


class EmotionVideoDatasetBuilder:
    def __init__(self, csv_path: str, output_dir: str = 'data/results'):
        self.csv_path = csv_path
        self.output_dir = output_dir
        self.logger = LoggerConfig.setup_logger(__name__)

        self.logger.info("=" * 70)
        self.logger.info("EMOTION VIDEO DATASET BUILDER INITIALIZED")
        self.logger.info("=" * 70)
        self.logger.info(f"CSV path: {csv_path}")
        self.logger.info(f"Output directory: {output_dir}")

        # Create directories
        self.logger.debug("Creating output directories...")
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        self.logger.debug("✅ Directories created/verified")

        # Initialize components
        self.logger.debug("Initializing QueryGenerator...")
        self.query_generator = QueryGenerator(csv_path)

        self.logger.debug("Initializing VideoScraper...")
        self.scraper = VideoScraper(rate_limit_delay=20.0, cookies_from_browser=("firefox",))

        self.logger.info("=" * 70)

    def generate_queries(self, style: str = 'simple') -> List[Dict]:
        """Genereer alle search queries"""
        self.logger.info("=" * 70)
        self.logger.info("STEP 1: QUERY GENERATION")
        self.logger.info("=" * 70)

        start_time = datetime.now()
        self.logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        queries = self.query_generator.generate_all_queries(style=style)

        if not queries:
            self.logger.error("❌ Query generation failed - no queries generated")
            return []

        # Save queries to JSON
        queries_file = f"{self.output_dir}/queries.json"
        self.logger.info(f"Saving queries to: {queries_file}")

        try:
            with open(queries_file, 'w', encoding='utf-8') as f:
                json.dump(queries, f, indent=2, ensure_ascii=False)

            file_size = os.path.getsize(queries_file)
            self.logger.info(f"✅ Queries saved successfully ({file_size / 1024:.2f} KB)")

        except Exception as e:
            self.logger.error(f"❌ Failed to save queries: {e}", exc_info=True)

        elapsed = datetime.now() - start_time
        self.logger.info(f"Query generation complete in {elapsed.total_seconds():.2f}s")
        self.logger.info("=" * 70 + "\n")

        return queries

    def scrape_all_queries(
        self,
        queries: List[Dict],
        platforms: List[str] = ['youtube'],
        start_from: int = 0,
        batch_size: int = 100
    ):
        """Scrape alle queries met batch processing"""
        self.logger.info("=" * 70)
        self.logger.info("STEP 2: VIDEO SCRAPING")
        self.logger.info("=" * 70)
        self.logger.info(f"Total queries: {len(queries):,}")
        self.logger.info(f"Platforms: {', '.join(platforms)}")
        self.logger.info(f"Starting from query: {start_from}")
        self.logger.info(f"Batch size: {batch_size}")
        self.logger.info("=" * 70)

        start_time = datetime.now()
        self.logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        all_results = []
        total_videos = 0
        queries_to_process = queries[start_from:]

        self.logger.info(f"Processing {len(queries_to_process):,} queries...")

        for idx, query_data in enumerate(queries_to_process, start=start_from):
            query_start = datetime.now()

            self.logger.info("")
            self.logger.info(f"{'=' * 70}")
            self.logger.info(f"Query [{idx + 1}/{len(queries)}] ({((idx + 1) / len(queries) * 100):.1f}%)")
            self.logger.info(f"Query: {query_data['query']}")
            self.logger.info(
                f"Emotion: {query_data['emotion']} | Subject: {query_data['subject']} | Setting: {query_data['setting']}"
            )
            self.logger.info(f"{'=' * 70}")

            try:
                # Scrape the query
                results = self.scraper.scrape_query(
                    query=query_data['query'],
                    platforms=platforms
                )

                # Add metadata
                results['query_id'] = query_data['id']
                results['emotion'] = query_data['emotion']
                results['subject'] = query_data['subject']
                results['setting'] = query_data['setting']
                results['timestamp'] = datetime.now().isoformat()

                all_results.append(results)
                total_videos += results.get('total_videos', 0)

                query_elapsed = (datetime.now() - query_start).total_seconds()
                self.logger.info(f"✅ Query processed in {query_elapsed:.2f}s - Found {results.get('total_videos', 0)} videos")
                self.logger.info(f"📊 Running total: {total_videos} videos from {len(all_results)} queries")

                # Save intermediate results every batch_size queries
                if (idx + 1) % batch_size == 0:
                    self.logger.info("")
                    self.logger.info("=" * 70)
                    self.logger.info(f"🔄 BATCH CHECKPOINT: {idx + 1} queries")
                    self.logger.info("=" * 70)

                    batch_file = f"results_batch_{idx + 1}.json"
                    self.save_results(all_results, batch_file)

                    # Log statistics
                    scraper_stats = self.scraper.get_stats()
                    self.logger.info("📊 Batch Statistics:")
                    self.logger.info(f"   Queries processed: {scraper_stats['queries_processed']}")
                    self.logger.info(f"   Total videos: {scraper_stats['total_videos_found']}")
                    self.logger.info(f"   YouTube videos: {scraper_stats['youtube_videos']}")
                    self.logger.info(f"   Vimeo videos: {scraper_stats['vimeo_videos']}")
                    self.logger.info(f"   Errors: {scraper_stats['errors']}")
                    self.logger.info(
                        f"   Avg videos/query: {scraper_stats['total_videos_found'] / max(scraper_stats['queries_processed'], 1):.2f}"
                    )

                    elapsed = (datetime.now() - start_time).total_seconds()
                    remaining_queries = len(queries) - (idx + 1)
                    avg_time_per_query = elapsed / max((idx + 1 - start_from), 1)
                    estimated_remaining = avg_time_per_query * remaining_queries

                    self.logger.info("⏱️  Time Statistics:")
                    self.logger.info(f"   Elapsed: {elapsed / 60:.1f} minutes")
                    self.logger.info(f"   Avg time/query: {avg_time_per_query:.2f}s")
                    self.logger.info(f"   Estimated remaining: {estimated_remaining / 60:.1f} minutes")
                    self.logger.info("=" * 70 + "\n")

            except Exception as e:
                self.logger.error(f"❌ Error processing query '{query_data['query']}': {e}", exc_info=True)

                error_result = {
                    'query': query_data['query'],
                    'query_id': query_data['id'],
                    'emotion': query_data['emotion'],
                    'subject': query_data['subject'],
                    'setting': query_data['setting'],
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                all_results.append(error_result)

        # Save final results
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("SAVING FINAL RESULTS")
        self.logger.info("=" * 70)
        self.save_results(all_results, "final_results.json")

        # Final statistics
        total_elapsed = (datetime.now() - start_time).total_seconds()
        scraper_stats = self.scraper.get_stats()

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("SCRAPING COMPLETE!")
        self.logger.info("=" * 70)
        self.logger.info(f"✅ Total queries processed: {len(all_results):,}")
        self.logger.info(f"✅ Total videos found: {total_videos:,}")
        self.logger.info(f"✅ YouTube videos: {scraper_stats['youtube_videos']:,}")
        self.logger.info(f"✅ Vimeo videos: {scraper_stats['vimeo_videos']:,}")
        self.logger.info(f"❌ Errors encountered: {scraper_stats['errors']}")
        self.logger.info(f"⏱️  Total time: {total_elapsed / 60:.1f} minutes ({total_elapsed / 3600:.2f} hours)")
        self.logger.info(f"📊 Average videos per query: {total_videos / max(len(all_results), 1):.2f}")
        self.logger.info(f"📊 Average time per query: {total_elapsed / max(len(all_results), 1):.2f}s")
        self.logger.info("=" * 70 + "\n")

        return all_results

    def save_results(self, results: List[Dict], filename: str) -> Optional[str]:
        """Save results to JSON (atomic write)"""
        filepath = os.path.join(self.output_dir, filename)
        tmp_filepath = filepath + ".tmp"

        self.logger.info(f"💾 Saving results to: {filepath}")
        self.logger.debug(f"   Number of results: {len(results)}")

        try:
            # Ensure output dir exists (in case this is called standalone)
            os.makedirs(self.output_dir, exist_ok=True)

            # Atomic write: write to temp then replace
            with open(tmp_filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

            os.replace(tmp_filepath, filepath)

            file_size = os.path.getsize(filepath)
            self.logger.info(f"✅ Saved successfully ({file_size / 1024:.2f} KB)")

            # Lightweight summary
            errors = sum(1 for r in results if isinstance(r, dict) and 'error' in r)
            total_videos = 0
            for r in results:
                if isinstance(r, dict):
                    total_videos += int(r.get('total_videos', 0) or 0)

            self.logger.info(f"📄 Summary in file:")
            self.logger.info(f"   Total items: {len(results):,}")
            self.logger.info(f"   Total videos (sum): {total_videos:,}")
            self.logger.info(f"   Error items: {errors:,}")

            return filepath

        except Exception as e:
            self.logger.error(f"❌ Failed to save results: {e}", exc_info=True)

            # Best-effort cleanup of tmp file
            try:
                if os.path.exists(tmp_filepath):
                    os.remove(tmp_filepath)
            except Exception:
                pass

            return None

    def run(
        self,
        style: str = "simple",
        platforms: List[str] = ['youtube'],
        start_from: int = 0,
        batch_size: int = 100
    ):
        """Convenience method: generate queries + scrape them."""
        queries = self.generate_queries(style=style)
        if not queries:
            self.logger.error("❌ Aborting run - no queries generated.")
            return []
        return self.scrape_all_queries(
            queries=queries,
            platforms=platforms,
            start_from=start_from,
            batch_size=batch_size
        )
