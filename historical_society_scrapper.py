"""
Scrapes weather data from climate.weather.gc.ca website.
"""
from html.parser import HTMLParser
import logging
import logging.handlers
import re
import datetime
import requests
from bs4 import BeautifulSoup
from os.path import abspath, dirname, join
import calendar
import time

from database_operations import DBOperations



class ManitobaHistoricalScrapper():
  """Scrapes info from """
  logger = logging.getLogger("main." + __name__)
  BASEURL = "http://www.mb1870.org/mhs-map/search?go=t&string1=&op2=AND&string2=&op3=AND&string3="
  urlMuni = "&m-name="
  urlType = "&st-name="
  urlEND = "&submit=Search"
  baseImageUrl = "http://www.mhs.mb.ca/docs/sites/images/"

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
        page = requests.get(self.get_url("", ""))
        soup = BeautifulSoup(page.content, "html.parser")
        try:
          fetchedMuni = soup.find("select", id="m-name")
          for muni in fetchedMuni.stripped_strings:
            if muni != "*ALL*":
              self.allMunicipality.append(muni.replace(" ", "+"))
        except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_varibles/Get Municipality: %s", error)

        try:
          fetchedType = soup.find("select", id="st-name")
          for type in fetchedType.stripped_strings:
            if type != "*ALL*":
              self.allTypes.append(type.replace(" ", "+"))
        except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_varibles/Get Types: %s", error)

      except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_varibles: %s", error)

  def get_site_links(self):
    """Gets all the links for the site"""
    #for muni in self.allMunicipality:
    #for type in self.allTypes:

    #Temp for testing
    siteType = "Building"
    try:
      url = self.get_url("Alexander", siteType)
      page = requests.get(url)
      soup = BeautifulSoup(page.content, "html.parser")

      try:
         for row in soup.tbody.find_all("tr"):
            columns = row.find_all("td")
            siteName = row.contents[0].text
            siteURL = row.contents[0].a["href"]
            siteMuni = row.contents[1].text
            siteAddress = row.contents[2].text
            """ print(siteName + ", " + siteMuni + ", " + siteAddress + ", " + siteURL)
            print("\n") """
            self.fetch_site_info(siteName, siteURL, siteMuni, siteAddress, siteType )

      except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_site_links/Loop through tr " + siteType + ": %s", error)



    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_site_links %s", error)

  def get_url(self, municipality, type):
    """Returns the properly formated url"""
    return self.BASEURL + "&m-name=" + municipality + "&st-name=" + type + "&submit=Search"


  def fetch_site_info(self, siteName, siteURL, siteMuni, siteAddress, siteType):
    """Gets the site information and save it to a dictionary"""
    try:
      page = requests.get(siteURL)
      soup = BeautifulSoup(page.content, "html.parser")
      relevantData = soup.find_all("table")[0].contents[4]

      siteDescription = ""
      siteLocation = ""
      sitePictures = []
      siteSources = []
      #Getting site description
      try:


        firstP = relevantData.find_all("p")[0]
        if "Link to:" not in firstP.text:
          siteDescription = firstP.text

        for p in firstP.next_siblings:
          if p.name != None and p.name != 'p':
                break

          if p.name == 'p':
            text = p.text + '\n'

            #Some sites didn't close the p tag, so this should cut the text off in those senerios
            if( "\n\n" in text):
                text = text.split("\n\n")[0]
            siteDescription += text

      except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Parse Description: %s \nUrl: " + siteURL, error)

      #Getting site pictures

      #Have to do this because some sites have multiple blockquotes, and the "photos" tag doesn't appear on sites with only one blockquote
      picStart = relevantData.find(id="photos")
      picBlock = relevantData.find_all("blockquote")[0]
      if picStart != None:
         picBlock = picStart.find_next_sibling("blockquote")


      picRelavant = picBlock.table.tr.td

      picLink = None
      picName = None

      for row in picRelavant.contents:
         try:
          #Might as well get the location while I'm at it
          if "Site" in row.text and "(lat/long):" in row.text:
              siteLocation = row.a.text
              break


          img = row.next_element

          if img.name == 'img':
              picLink = img['src']
              try:
                 img_full_url = self.baseImageUrl + picLink.split("images/")[1]
                 img_data = requests.get(img_full_url).content

                 #get image name with unique timestamp, downloads to folder
                 picName = picLink[picLink.find('images/')+7 : picLink.find('.')] + "_" + str(calendar.timegm(time.gmtime())) + "." + picLink.split(".")[1]
                 imagePath = join(dirname(abspath(__file__)), "Site_Images", picName)
                 with open(imagePath, 'wb') as handler:
                     handler.write(img_data)
              except Exception as error:
                self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Download Image:  %s \nUrl: " + siteURL, error)

          elif picLink != None and row.text != '\n':
             sitePictures.append(dict(link = picLink, name = picName, info = row.text))
             picLink = None
             picName = None


         except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Parse Image:  %s \nUrl: " + siteURL, error)



      #Getting Site Sources

      #Fetch the H2 tag that signifys the sources
      sourceStart = soup.find(id="sources")

      currentSource = sourceStart.find_next_sibling('p')

      for loop in range(20):
          try:
            #If end of sources, exit loop
            if "This page was" in currentSource.text and "prepared by" in currentSource.text:
              break
            siteSources.append(dict(info = currentSource.text))
            #Get next source
            currentSource = currentSource.find_next_sibling('p')
          except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Parse Sources: %s \nUrl: " + siteURL, error)

      latitude = None
      longitude = None
      firstType = [siteType]
      try:
        latitude = float(siteLocation.split(', ')[0].replace("N", "").replace("S","-"))
        longitude = float(siteLocation.split(', ')[1].replace("W", "-").replace("E", ""))
      except Exception as error:
            latitude = None
            longitude = None
            self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Get Location: %s \nUrl: " + siteURL, error)
      self.allSites.append(dict(site_name = siteName, types = firstType, municipality =  siteMuni, address = siteAddress, latitude = latitude, longitude = longitude, description = siteDescription, pictures  = sitePictures, sources = siteSources, url = siteURL))
    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/fetch_site_info: %s \nUrl: " + siteURL, error)











if __name__ == "__main__":
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)
    file_handler = logging.handlers.RotatingFileHandler(filename="historical_society_scrapper.log",
                                                  maxBytes=10485760,
                                                  backupCount=10)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("Application Historical Society Scrapper Started")
    siteScraper = ManitobaHistoricalScrapper()
    siteScraper.get_all_varibles()
    #print(siteScraper.allMunicipality)
    #print(siteScraper.allTypes)
    siteScraper.get_site_links()
    #siteScraper.fetch_site_info("St. John Ukrainian Greek Orthodox Church and Cemetery", "http://www.mhs.mb.ca/docs/sites/stjohncemeterystead.shtml", "Alexander", "Stead", "Building")
    print(siteScraper.allSites[0]["site_name"])

    logger.info("Application Insert Data into Database")
    database = DBOperations()
    database.initialize_db()
    database.purge_data()
    database.manitoba_historical_website_save_data(siteScraper.allSites)






