Copernicus Sentinel Science Hub rolling archive downloader
==========================================================

This is an easy script to download Sentinel data from the COPERNICUS rolling archive
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
 * a simple YAML file format can be used to improve and customize
   the script configuration
 * Files are downloaded in subtrees in order to avoid giant directories when
   large spatial and time series are downloaded.
 * All management and download operations can be run separately
 * data downloads can be checked and restarted on failure
 * it supports Copernicus mirrors, thanks to @realm convention in authentication
 * it support continuous download or can be used via cron
 * it is free software and can be extended easily for better purposes
 * it is currently used in production and it works!

Currently, the main use case of this script is the periodic downloading
of images on one ore more areas (sites), typically by means of a
crontab/timer job.

What follows is an example of configuration YAML file used by this script:

```
  ---

  all:
    platform: S-1
    type: GRD
    direction: Any
    items:
      - aoi:
        polygon: POLYGON ((144.26 -32.81,147.86 -32.81,147.86 -35.998,144.26 -35.998,144.26 -32.81))
        directory: /tmp/data/Sentinel-1_archive/Yanco_archive

      - aoi:
        polygon: POLYGON ((-100.55 50.67,-96.35 50.67,-96.35 47.762,-100.55 47.762,-100.55 50.67))
        directory: /tmp/data/Sentinel-1_archive/ElmCreek_archive

      - aoi:
        polygon: POLYGON ((-100.79 36.12,-97.582 36.12,-97.582 34.052,-100.79 34.052,-100.79 36.12))
        directory: /tmp/data/Sentinel-1_archive/LittleWashita_archive

	  - aoi:
		polygon: POLYGON ((20.53 54.95,25.158 54.95,25.158 51.942,20.53 51.942,20.53 54.95))
		directory: /tmp/data/Sentinel-1_archive/Poland_archive

	  - aoi:
		polygon: POLYGON ((8.15446235928205 57.6797621069406,10.7701315198645 57.672559958462,10.7663436347965 53.719444453071,7.71 53.9916504060973,8.15446235928205 57.6797621069406))
		directory: /tmp/data/Sentinel-1_archive/Hobe_archive

	  - aoi:
		polygon: POLYGON ((-99.77 31.62,-96.662 31.62,-96.662 29.4,-99.77 29.4,-99.77 31.62))
		directory: /tmp/data/Sentinel-1_archive/TxSON_archive

   	  - aoi:
		polygon: POLYGON ((5.9532 52.0676,7.048 52.032,6.9952 49.948,5.9 49.9304,5.9532 52.0676))
		directory: /tmp/data/SARSENSE_S1/Germany

	  - aoi:
		polygon: POLYGON ((7.679771707123388 45.488568380045756,13.466264982898075 46.723232221305132,36.010241064894799 37.582947071503241,36.314421645072912 32.184725616868526,31.963195382196588 29.687765362998697,18.909516593567609 29.41959521817747,9.110692096066586 33.050217585529545,8.699552606660951 35.428401418400881,-5.793114394887761 34.502145739241485,-9.709678262248175 38.23185231330713,-8.818961263123319 43.620276606196732,7.679771707123388 45.488568380045756))
		direction: ascending
		directory: /tmp/Mediterraneo

```

This file should be stored as `/usr/local/etc/scihub.yml`, while `$HOME/.scihub.yml` 
should contain the user's credentials, such as

 username: foo@apihub.esa.int
 password: mysupersecretpasswd

