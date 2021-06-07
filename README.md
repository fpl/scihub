Copernicus Sentinel Science Hub rolling archive downloader
==========================================================

This is an easy script to download Sentinel data from the ESA rolling archive
located at the Copernicus Scientific Data Hub ttps://scihub.copernicus.eu/

The archive can be queryed by a custom web service API documented at
https://scihub.copernicus.eu/userguide/5APIsAndBatchScripting
and this script adopt it to do a better job in downloading periodically
and storing images. The API is based on OpenData http://www.odata.org/
and OpenSearch http://www.opensearch.org/

Instead of re-inventing the wheel, the current version of this script
uses the Python API provided by the `sentinelsat` package to query the archive
and download images. This script has some useful features which can
be useful for regular (batch) downloads and are not currently available
in sentinelsat per se. Specifically:

 * a local Spatialite database is used to know if images already have
   been downloaded or not, and to store a set of geospatial metadata
   to query the archive with ordinary OGC SQL ST functions.
 * it creates useful KML add-on files filled with information taken from imagery metadata
 * it downloads the official manifest file along with (optionally) image kit
 * a simple YAML file format can be used to improve and customize
   the script
 * All management and download operations can be run separately
 * data downloads can be checked and restarted on failure
 * it supports DHuS mirrors, thanks to @realm convention in authentication
 * it support continuous download or can be used via cron
 * it is free software and can be extended easily for better purposes
 * it is currently used in production and it works!

Currently, the main use case of this script is the periodic downloading
of images on one ore more areas (sites), typically by means of a
crontab/timer job.

What follows is an example of configuration YAML file used by this script:

	--

	date: 2014-11-10
	directory: /data/main/sentinel/archive

	- site: 
    	name: First
		geometry: POLYGON((15.819485626219 40.855620164394,16.445706329344 40.855620164394,16.445706329344 41.120994219991,15.819485626219 41.120994219991,15.819485626219 40.855620164394))
		date: 2018-04-21
		directory: /home/foo
		direction: ascending
		- product:
			platform: Sentinel-1
			type: GRDH
			direction: ascending
			directory: /home/foo/sentinel-1
			date: 2018-04-21 
		- product:
			platform: Sentinel-2
			type: MSIL2
			cloudcoverpercentage: 10
			direction: any
			directory: /home/foo/sentinel-2
			date: 2020-04-12
		- product:
			platform: Sentinel-1
			type: SLC

	- site: 
		name: Second
		geometry: POLYGON((16.349232635497 40.791189284951,16.909535369872 40.791189284951, \
			16.909535369872 41.131338714384,16.349232635497 41.131338714384,16.349232635497 40.791189284951))
		directory: /data/scihub/archive
		- product:
			platform: S1 
			type: GRDH
			direction: asc
			directory: $SOME_ENVIRONMENT_VARIABLE/sentinel/data
			date: 2019-11-19
		- product:
			platform: S2
			type: MSIL2
			cloudcoverpercentage: 20
			direction: descending
			date: 2020-06-11
			directory: $PWD

This file can be stored as `/usr/local/etc/scihub.yml` or `$HOME/.scihub.yml` or
splitted among them, as more convenient.
