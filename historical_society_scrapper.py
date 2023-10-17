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
import asyncio

from database_operations import DBOperations



class ManitobaHistoricalScrapper():
  """Scrapes info from """
  logger = logging.getLogger("main." + __name__)
  BASE_SEARCH_URL = "http://www.mb1870.org/mhs-map/search?go=t&string1=&op2=AND&string2=&op3=AND&string3="
  urlMuni = "&m-name="
  urlType = "&st-name="
  urlEND = "&submit=Search"
  baseSiteImageUrl = "http://www.mhs.mb.ca/docs/sites/images/"
  baseFeatureImageURL = "http://www.mhs.mb.ca/docs/features/"
  baseImageURL = "http://www.mhs.mb.ca/docs/"

  #Urls that are not included because they are not a phyiscal site
  excludedNonSitesURLs = ["http://www.mhs.mb.ca/docs/sites/mhswarmemorial.shtml"]

  #Urls where the page is so broken I would have to rewrite my entire code for these edge casses
  excludedProblematicUrls = ["http://www.mhs.mb.ca/docs/sites/woodlandsmuseum.shtml"]

  def __init__(self):
    try:
        #self.allMunicipality = []
        self.allTypes = ["Building", "Cemetery", "Featured+site","Location","Monument","Museum%2FArchives", "Other"]
        self.allSites = []
        self.saveImages = True
        self.errorCount = 0
        self.badSites = []


    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/init: %s", error)


  # def get_all_varibles(self):
  #     """Gets the Municipality and site types used """
  #     #Now unsed
  #     try:
  #       page = requests.get(self.get_url("", ""))
  #       soup = BeautifulSoup(page.content, "html.parser")
  #       try:
  #         fetchedMuni = soup.find("select", id="m-name")
  #         for muni in fetchedMuni.stripped_strings:
  #           if muni != "*ALL*":
  #             self.allMunicipality.append(muni.replace(" ", "+"))
  #       except Exception as error:
  #           self.logger.error("ManitobaHistoricalScrapper/get_all_varibles/Get Municipality: %s", error)
  #           self.errorCount += 1

  #       try:
  #         fetchedType = soup.find("select", id="st-name")
  #         for type in fetchedType.stripped_strings:
  #           if type != "*ALL*":
  #             self.allTypes.append(type.replace(" ", "+").replace("/", "%2F"))
  #       except Exception as error:
  #           self.logger.error("ManitobaHistoricalScrapper/get_all_varibles/Get Types: %s", error)
  #           self.errorCount += 1

  #     except Exception as error:
  #           self.logger.error("ManitobaHistoricalScrapper/get_all_varibles: %s", error)
  #           self.errorCount += 1

  def check_if_duplicate_site(self, siteURL, siteType):
    """Checks if the site has already been processed. Reason being a site with multiple types would be added twice. This way, it will instead add the new type to the existing entry in  allSites"""
    duplicate = False
    try:
      for index in range(len(self.allSites)):
         if self.allSites[index]["url"] == siteURL:
            duplicate = True
            self.allSites[index]["types"].append(siteType)
            break

    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/check_if_duplicate_site %s", error)
            self.errorCount += 1
    return duplicate

  def get_all_sites(self):
     """Gets all sites"""
     try:
        for siteType in self.allTypes:
           self.get_all_site_links_for_type(siteType)

     except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_sites: %s", error)
            self.errorCount += 1


  def get_all_site_links_for_type(self, siteType):
    """Gets all the links for the site"""
    #for muni in self.allMunicipality:
    #for type in self.allTypes:


    try:
      url = self.get_url("", siteType)
      page = requests.get(url)
      soup = BeautifulSoup(page.content, "html.parser")
      numOfSitesPre = len(self.allSites)


      for row in soup.tbody.find_all("tr"):
         try:
            checkSiteLink = row.contents[0].find_all("a")
            if len(checkSiteLink) > 0:
              columns = row.find_all("td")
              siteName = row.contents[0].text
              siteURL = row.contents[0].a["href"]
              siteMuni = row.contents[1].text
              siteAddress = row.contents[2].text
              """ print(siteName + ", " + siteMuni + ", " + siteAddress + ", " + siteURL)
              print("\n") """
              if self.check_if_duplicate_site(siteURL, siteType) == False and siteURL not in self.excludedNonSitesURLs and siteURL not in self.excludedProblematicUrls:
                self.fetch_site_info(siteName, siteURL, siteMuni, siteAddress, siteType )


         except Exception as error:
              self.logger.error("ManitobaHistoricalScrapper/get_all_site_links_for_type/Loop through tr " + siteType + ": %s", error)
              self.errorCount += 1

      numOfSitesPost = len(self.allSites)
      print(str(numOfSitesPost - numOfSitesPre) + " " + siteType + " found")



    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_site_links_for_type %s", error)
            self.errorCount += 1

  def get_url(self, municipality, type):
    """Returns the properly formated url"""
    return self.BASE_SEARCH_URL + "&m-name=" + municipality + "&st-name=" + type + "&submit=Search"


  def fetch_site_info(self, siteName, siteURL, siteMuni, siteAddress, siteType):
    """Gets the site information and save it to a dictionary"""
    startError = self.errorCount
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
            self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Parse Description: %s \nUrl: " + siteURL + "\n", error)
            self.errorCount += 1

      #Getting site pictures

      #Have to do this because some sites have multiple blockquotes, and the "photos" tag doesn't appear on sites with only one blockquote. Some have the id "photos" some do not
      picStart = relevantData.find(id="photos")
      if picStart == None:
         picStart = relevantData.find("h2", string="Photos & Coordinates" )
      picBlock = relevantData.find_all("blockquote")[0]
      picRelavant = None
      try:
        if picStart != None:
          picBlock = picStart.find_next_sibling("blockquote")
        picRelavant = picBlock.table.tr.td
      except Exception as error:
            if startError == self.errorCount:
              self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Get Image Block: %s \nUrl: " + siteURL + "\n", error)
            self.errorCount += 1

      picLink = None
      fileName = None
      img_full_url = None

      for row in picRelavant.contents:
         try:
          #Might as well get the location while I'm at it
          if "Site" in row.text and "(lat/long):" in row.text:
              siteLocation = row.a.text.replace("\t", " ")
              break


          img = row.next_element

          if img.name == 'img':
              picLink = img['src']
              try:
                  picName = picLink.split("/")[-1]
                  if "../" in picLink:
                      websitePath = picLink.split("../")[1]
                      img_full_url = self.baseImageURL + websitePath
                  else:
                     img_full_url = self.baseSiteImageUrl + picName

                  # if "features/" in picLink:
                  #    img_full_url = self.baseFeatureImageURL + picLink.split("features/")[1]
                  # else:
                  #   img_full_url = self.baseSiteImageUrl + picName

                  img_data = requests.get(img_full_url).content


                  #get image name with unique timestamp, downloads to folder
                  #fileName = picLink[picLink.find('images/')+7 : picLink.find('.')] + "_" + str(calendar.timegm(time.gmtime())) + "." + picLink.split(".")[1]
                  firstPartOfName = siteURL.split("/")[-1]
                  fileName =  firstPartOfName.split(".")[1] + "-" + picName.split(".")[0] + "_" + str(calendar.timegm(time.gmtime())) + "." + picName.split(".")[1]
                  imagePath = join(dirname(abspath(__file__)), "Site_Images", fileName)
                  if self.saveImages:
                    with open(imagePath, 'wb') as handler:
                        handler.write(img_data)
              except Exception as error:
                if startError == self.errorCount:
                  self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Download Image:  %s \nUrl: " + siteURL + "\n", error)
                self.errorCount += 1

          elif picLink != None and row.text != '\n':
             sitePictures.append(dict(link = img_full_url, name = fileName, info = row.text))
             picLink = None
             fileName = None


         except Exception as error:
            if startError == self.errorCount:
              self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Parse Image:  %s \nUrl: " + siteURL + "\n", error)
            self.errorCount += 1



      #Getting Site Sources

      #Fetch the H2 tag that signifys the sources

      try:
        sourceStart = relevantData.find(id="sources")
        if sourceStart == None:
           sourceStart = relevantData.find("h2", string="Sources:" )
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
              if startError == self.errorCount:
                self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Parse through Sources: %s \nUrl: " + siteURL + "\n", error)
              self.errorCount += 1
      except Exception as error:
        if startError == self.errorCount:
          self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Get Sources: %s \nUrl: " + siteURL + "\n", error)
        self.errorCount += 1

      latitude = None
      longitude = None
      firstType = [siteType]
      try:
        latitude = float(siteLocation.split(', ')[0].replace("N", "").replace("S","-"))
        longitude = float(siteLocation.split(', ')[1].replace("W", "-").replace("E", ""))
      except Exception as error:
            latitude = None
            longitude = None
            if startError == self.errorCount:
              self.logger.error("ManitobaHistoricalScrapper/fetch_site_info/Get Location: %s \nUrl: " + siteURL + "\n", error)
            self.errorCount += 1
      if self.errorCount > startError:
       self.badSites.append(siteURL)
      else:
        self.allSites.append(dict(site_name = siteName, types = firstType, municipality =  siteMuni, address = siteAddress, latitude = latitude, longitude = longitude, description = siteDescription, pictures  = sitePictures, sources = siteSources, url = siteURL))
    except Exception as error:
            if startError == self.errorCount:
              self.logger.error("ManitobaHistoricalScrapper/fetch_site_info: %s \nUrl: " + siteURL + "\n", error)
            self.errorCount += 1













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
    startTime = datetime.datetime.now()
    siteScraper = ManitobaHistoricalScrapper()
    siteScraper.saveImages = False

    #print(siteScraper.allMunicipality)
    #print(siteScraper.allTypes)
    #asyncio.run(siteScraper.get_all_site_links_for_type("Museum%2FArchives"))
    #siteScraper.get_all_site_links_for_type("Museum%2FArchives")
    #siteScraper.fetch_site_info("St. John Ukrainian Greek Orthodox Church and Cemetery", "http://www.mhs.mb.ca/docs/sites/gabrielleroyhouse.shtml", "Alexander", "Stead", "Building")
    #print(siteScraper.allSites[0]["site_name"])
    print("Start fetching data at " + str(startTime))
    #siteScraper.get_all_sites()
    siteScraper.get_all_site_links_for_type("Building")
    endTime = datetime.datetime.now()

    print("# of error fetching data: " + str(siteScraper.errorCount))
    print("# of bad sites " + str(len(siteScraper.badSites)))
    print("Completed fetching data at " + str(endTime))
    print("Time it took to fetch data: " + str(endTime - startTime))

    logger.info("Insert Data into Database")
    database = DBOperations()
    database.initialize_db()
    database.purge_data()
    database.manitoba_historical_website_save_data(siteScraper.allSites)






