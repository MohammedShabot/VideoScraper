from vimeo_scraper import VimeoScraper


if __name__ == '__main__':
    scraper = VimeoScraper(rate_limit_delay=30.0, use_selenium=True)

    videos = scraper.search(max_results=5)