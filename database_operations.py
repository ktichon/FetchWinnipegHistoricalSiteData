
import logging
from dbcm import DBCM

class DBOperations:
    """Store and retrive site data"""

    logger = logging.getLogger("main." + __name__)

    def __init__(self):
        """Initializes varibles that will be used throught the class"""
        self.database = "historicalSiteData.sqlite"


    def initialize_db(self):
        """Initializes the database"""
        with DBCM(self.database) as cursor:
            try:
                cursor.execute("""create table if not exists historicalSite
                (site_id integer primary key autoincrement not null,
                name text,
                streetName text,
                streetNumber text,
                constructionDate text,
                shortUrl text,
                longUrl text,
                latitude real,
                longitude real,
                city text,
                province text,
                municipality text,
                description text,
                type text
                );""")

                cursor.execute("""create table if not exists sitePhotos
                (photo_id integer primary key autoincrement not null,
                site_id int,
                photo BLOB,
                description text,
                date text
                );""")
                cursor.execute("""create table if not exists siteSource
                (source_id integer primary key autoincrement not null,
                site_id int,
                info text,
                );""")
            except Exception as error:
                self.logger.error('DBOperations/initialize_db: %s', error)

    def purge_data(self):
      """Removes all data from the db"""
      with DBCM(self.database) as cursor:
          try:
              cursor.execute("""DELETE FROM historicalSite;""")
          except Exception as error:
              self.logger.error('DBOperations/purge_data: %s', error)



    def winnipeg_api_save_data(self, historical_sites_list):
        """Saves a dictionary of historical sites  values to the database"""
        try:
            insert_sql =  """INSERT OR IGNORE into historicalSite
            (name, streetName, streetNumber, constructionDate, shortUrl, longUrl, latitude, longitude,  city, province, municipality, description, type)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""



            with DBCM(self.database) as cursor:
                try:
                    before_insert = cursor.execute("SELECT COUNT() FROM historicalSite").fetchone()[0]
                    cursor.executemany(insert_sql, historical_sites_list)
                    after_insert = cursor.execute("SELECT COUNT() FROM historicalSite").fetchone()[0]
                    print("Inserted " + str(after_insert - before_insert) + " new rows")
                except Exception as error:
                    self.logger.error('DBOperations/save_data/Insert Into database: %s', error)
        except Exception as error:
            self.logger.errorint('DBOperations/save_data: %s', error)

    def manitoba_historical_website_save_data(self, new_historical_site):
        """Saves the data from the Manitoba Historical Society one at a time"""
        try:
            sql = """SELECT TOP 1 site_id FROM historicalSite WHERE streetName = ? AND streetNumber = ?"""
            site_id = None
            with DBCM(self.database) as cursor:
                try:
                    site_id = cursor.execute(sql, (new_historical_site["street_name"],new_historical_site["street_number"] )).fetchone

                except Exception as error:
                    self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database: %s', error)




        except Exception as error:
            self.logger.errorint('DBOperations/manitoba_historical_website_save_data: %s', error)

