ESA Sentinel-1 SCIHUB rolling archive downloader
================================================

This is an easy script to download Sentinel-1 data from the ESA rolling archive
located at the Scientific Data Hub https://scihub.esa.int/

The archive can be queryed by a custom web service API documented at
https://scihub.esa.int/userguide/BatchScripting
and this script adopt it to do a better job in downloading periodically
and storing images. The API is based on OpenData http://www.odata.org/
and OpenSearch http://www.opensearch.org/


This Python script can be installed to download regularly from the
archive on the basis of a specific query. It is a proof of concept,
but it is quite complete, thanks to a series of features:

 * a local SQLite database is used to know if images already have
   been downloaded or not
 * it parses accurately XML results to find useful information
 * it creates useful KML add-on files filled with information taken from imagery metadata
 * it downloads the official manifest file along with (optionally) image kit
 * a simple INI file format can be used to improve and customize
   the script
 * all management and download operations can be run separately
 * it is free software and can be extended easily for better purposes
 * it is currently used in production and it works!

What follows is an example of configuration .cfg INI file used by this script:

	;
	; Polygons, types and directions need to be coherent each other and list
	; the same number of items.
	;
	
	[Polygons]
	
	polygon1 = POLYGON((15.819485626219 40.855620164394,16.445706329344 40.855620164394,16.445706329344 41.120994219991,15.819485626219 41.120994219991,15.819485626219 40.855620164394))
	polygon2 = POLYGON((16.349232635497 40.791189284951,16.909535369872 40.791189284951,16.909535369872 41.131338714384,16.349232635497 41.131338714384,16.349232635497 40.791189284951))
	
	[Types]
	
	type1 = SLC
	type2 = SLC
	
	[Directions]
	
	direction1 = Descending
	direction2 = Ascending
	
	[Authentication]
	username = XXXXXXXX
	password = YYYYYYY

This file can be stored as `/usr/local/etc/scihub.cfg` or `$HOME/.scihub.cfg` or
splitted among them, as more convenient.
   
Note that you need to install a few pre-requisite packages in order to use this script,
mainly pycurl, ogr, ElementTree and urllib.

