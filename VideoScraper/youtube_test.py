from youtube_scraper import VideoScraper


if __name__ == '__main__':
    scraper = VideoScraper(rate_limit_delay=30.0)

    scraper.run_scraper()