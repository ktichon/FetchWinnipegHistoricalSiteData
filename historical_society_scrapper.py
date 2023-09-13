"""
Scrapes weather data from climate.weather.gc.ca website.
"""
from html.parser import HTMLParser
import logging
import re
import datetime
import requests
from bs4 import BeautifulSoup


class ManitobaHistoricalScrapper():
  """Scrapes info from """
  logger = logging.getLogger("main." + __name__)
  BASEURL = "http://www.mb1870.org/mhs-map/search?go=t&string1=&op2=AND&string2=&op3=AND&string3="
  urlMuni = "&m-name="
  urlType = "&st-name="
  urlEND = "&submit=Search"

  def __init__(self):
    try:
        self.muni = None
        self.type = None
        self.url = self.BASEURL + self.urlMuni + self.urlType + self.urlEND
        self.allMunicipality = []
        self.allTypes = []


    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/init: %s", error)

  def get_all_varibles(self):
      """Gets the Municipality and site types used """
      try:
        page = requests.get(self.url)
        soup = BeautifulSoup(page.content, "html.parser")
        try:
          fetchedMuni = soup.find("select", id="m-name")
          for muni in fetchedMuni.stripped_strings:
            if muni != "*ALL*":
              self.allMunicipality.append(muni.replace(" ", "+"))
        except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_varibles:Get Municipality %s", error)

        try:
          fetchedType = soup.find("select", id="st-name")
          for type in fetchedType.stripped_strings:
            if type != "*ALL*":
              self.allTypes.append(type.replace(" ", "+"))
        except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_varibles:Get Types %s", error)

      except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_varibles: %s", error)

  def get_site_links(self):
     """Gets all the links for the site"""
     #for muni in self.allMunicipality:
     #for type in self.allTypes:









if __name__ == "__main__":
    siteScraper = ManitobaHistoricalScrapper()
    siteScraper.get_all_varibles()
    print(siteScraper.allMunicipality)
    print(siteScraper.allTypes)






