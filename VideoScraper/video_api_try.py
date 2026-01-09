from bs4 import BeautifulSoup
import requests
from yt_dlp import YoutubeDL
import csv
from tools import read_csv
from itertools import cycle
import time
import logging



class VideoAPI():

    def __init__(self):
        self.logger = logging.getLogger("__name__")
        logging.basicConfig(filename='scraping.log', encoding='utf-8', level=logging.DEBUG)



    #Expects a duckduckgo url
    def get_sources(self, url: str):
        '''
        This method parses an URL using BeautifulSoup to extract all the links of the videos present in the HTML
        It does this through finding all iframes, looping over them and extracting the 'src' tag inside each iframe
        This can later be used to download video's using said link
        Returns empty list if an error (Network or unexpected error) occurs
        
        :param url: The URL that has to be scraped for iframes
        :type url: str

        :return sources: List of sources that can be used to download the video's
        :type sources: List[str] | []
        '''

        self.logger.info("Started scraping: ", url)
        try:
            response = requests.get(url)

            #Parse HTML of the search query url
            soup = BeautifulSoup(response.text, 'html.parser')
            self.logger.info("Created the beautifulsoup instance using URL")

            #Save a list of all iframes that are available through the parsed HTML
            iframes = soup.find_all('iframe')
            self.logger.info("Found all of the iframes in the HTML of URL")
            #Loop over all iframes found in the HTML parses and extract the links to be passed to yt-dlp later
            sources = []
            for iframe in iframes:
                sources.append(iframe.get('src'))
            
            self.logger.info("Extracted all the src tags from the URL")

            return sources
        
        except requests.RequestException as e:
            print(f"Network error occured", e)
            self.logger.error(f"Network error occured", e)
            return []
        
        except Exception as e:
            print(f"An unexpected error occured:", e)
            self.logger.error(f"An unexpected error occured: ", e)
            return []
        


    def download_sources(src: str):


    