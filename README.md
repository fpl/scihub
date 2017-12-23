Copernicus Sentinel Science Hub rolling archive downloader
==========================================================

[ _You could be also interested in https://github.com/fpl/scihub-qgis-plugin_ ]


This is an easy script to download Sentinel data from the ESA rolling archive
located at the Copernicus Scientific Data Hub ttps://scihub.copernicus.eu/

The archive can be queryed by a custom web service API documented at
https://scihub.copernicus.eu/userguide/5APIsAndBatchScripting
and this script adopt it to do a better job in downloading periodically
and storing images. The API is based on OpenData http://www.odata.org/
and OpenSearch http://www.opensearch.org/

This Python script can be installed to download regularly from the
archive on the basis of a specific query. It is a proof of concept,
but it is quite complete, thanks to a series of features:

 * a local SQLite database is used to know if images already have
   been downloaded or not, and to store a set of metadata
 * it parses accurately XML results to find useful information
 * it creates useful KML add-on files filled with information taken from imagery metadata
 * it downloads the official manifest file along with (optionally) image kit
 * a simple INI file format can be used to improve and customize
   the script
 * All management and download operations can be run separately
 * data downloads can be checked and restarted on failure
 * it supports DHuS mirrors, thanks to @realm convention in authentication
 * it support continuous download or can be used via cron
 * it is free software and can be extended easily for better purposes
 * it is currently used in production and it works!

Currently, the main use case of this script is the periodic downloading
of images on one ore more areas, typically by means of a crontab job.

What follows is an example of configuration .cfg INI file used by this script:

	[Global]

	;
	; This sections contains default specs for parameters which can be
	; overriden for each polygon item
	;

	platform = Sentinel-1
	type = SLC
	direction = Any
	directory = /home/user/my/dir

	; this is only useful with S-2 images
	cloudcoverpercentage = 10 ; default 5

	;
	; Polygons, types, platforms and directions need to be coherent each other and list
	; the same number of items.
	;
	
	[Polygons]
	
	polygon1 = POLYGON((15.819485626219 40.855620164394,16.445706329344 40.855620164394,16.445706329344 41.120994219991,15.819485626219 41.120994219991,15.819485626219 40.855620164394))
	polygon2 = POLYGON((16.349232635497 40.791189284951,16.909535369872 40.791189284951,16.909535369872 41.131338714384,16.349232635497 41.131338714384,16.349232635497 40.791189284951))
	polygon2 = POLYGON((16.349232635497 40.791189284951,16.909535369872 40.791189284951,16.909535369872 41.131338714384,16.349232635497 41.131338714384,16.349232635497 40.791189284951))
	
	[Platforms]

	platform1 = Sentinel-1
	platform2 = Sentinel-2
	platform3 = ANY

	[Types]
	
	type1 = SLC
	type2 = MSIL1
	type3 = Any
	
	[Directions]
	
	direction1 = Descending
	direction2 = Ascending
	direction3 = ANY

	[Directories]

	directory1 = /home/user/dir1
	directory2 = $PWD
	directory3 = $SOME_ENVIRONMENT_VARIABLE/sentinel/data

	
	[Authentication]
	username = XXXXXXXX@apihub.esa.int
	password = YYYYYYY

This file can be stored as `/usr/local/etc/scihub.cfg` or `$HOME/.scihub.cfg` or
splitted among them, as more convenient.
   
Note that you need to install a few pre-requisite packages in order to use this script,
mainly pycurl, ogr, ElementTree and urllib.

