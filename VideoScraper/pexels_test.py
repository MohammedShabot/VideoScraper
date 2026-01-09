import asyncio
from pexels_scraper import PexelsScraper

async def main():
    scraper = PexelsScraper()
    await scraper.run_scraper()

if __name__ == "__main__":
    asyncio.run(main())
