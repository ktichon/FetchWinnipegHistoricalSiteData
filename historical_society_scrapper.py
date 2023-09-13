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
        self.allMunicipality = []
        self.allTypes = []
        self.allSites = []


    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/init: %s", error)

  def get_all_varibles(self):
      """Gets the Municipality and site types used """
      try:
        page = requests.get(self.getUrl("", ""))
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

    #Temp for testing
    siteType = "Building"
    try:
      url = self.getUrl("Alexander", siteType)
      page = requests.get(url)
      soup = BeautifulSoup(page.content, "html.parser")

      try:
         for row in soup.tbody.find_all("tr"):
            columns = row.find_all("td")
            siteName = row.contents[0].text
            siteLink = row.contents[0].a["href"]
            siteMuni = row.contents[1].text
            siteAddress = row.contents[2].text
            print(siteName + ", " + siteMuni + ", " + siteAddress + ", " + siteLink)

            """ for column in row.contents:
              print(column) """
            print("\n")

      except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_site_links:Loop through tr %s", error)



    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_site_links %s", error)

  def getUrl(self, municipality, type):
    """Returns the properly formated url"""
    return self.BASEURL + "&m-name=" + municipality + "&st-name=" + type + "&submit=Search"


  def fetchSiteInfo(self, siteName, siteLink, siteMuni, siteType)
    """Gets the site information and save it to a dictionary"""










if __name__ == "__main__":
    siteScraper = ManitobaHistoricalScrapper()
    siteScraper.get_all_varibles()
    #print(siteScraper.allMunicipality)
    #print(siteScraper.allTypes)
    siteScraper.get_site_links()






