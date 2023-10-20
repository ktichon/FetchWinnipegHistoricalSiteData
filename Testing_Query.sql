-- SQLite
SELECT mhs.id, mhs.name, photo_name, mhs.site_url, photo_url
--SELECT COUNT( photo_url)
FROM manitobaHistoricalSite mhs
INNER JOIN sitePhotos sp ON sp.site_id = mhs.id
where photo_name like '%nophoto%'
