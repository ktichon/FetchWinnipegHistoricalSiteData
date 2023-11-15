-- SQLite
-- SELECT mhs.site_id, mhs.name,  COUNT(st.siteType_id)

-- FROM manitobaHistoricalSite mhs
-- INNER JOIN siteType st on st.site_id = mhs.site_id
-- GROUP by mhs.site_id, mhs.name
-- HAVING COUNT(st.siteType_id) > 1

--SELECT * FROM  siteType
--WHERE site_id = 1012

--SELECT DISTINCT  municipality FROM manitobaHistoricalSite

SELECT DISTINCT municipality FROm manitobaHistoricalSite --where municipality = '`'





