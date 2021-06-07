#!/usr/bin/env python3
#
#   Copyright (C) 2015-2018 Francesco P. Lovergine <francesco.lovergine@cnr.it>
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#
# SciHub documentation can be read here:
# https://scihub.esa.int/userguide/BatchScripting
#
# An example of .cfg inifile used by this script:
#	 [Global]
#
#	 ;
#	 ; This sections contains default specs for parameters which can be
#	 ; overriden for each polygon item
#	 ;
#
#	 platform = Sentinel-1
#	 type = SLC
#	 direction = Any
#
#    directory = /my/own/path/for/products
#
#    ; you can also use any environment variable that will be expanded before
#    ; use e.g.
#    ; directory = $PWD
#    ; An empty value means current working directory, as well
#
#	 ; this is only useful with S-2 images
#
#	 cloudcoverpercentage = 10
#
#    ;
#    ; Polygons, types and directions need to be coherent each other and list
#    ; the same number of items.
#    ;
#    
#    [Polygons]
#    
#    polygon1 = POLYGON((15.819485626219 40.855620164394,16.445706329344 40.855620164394,16.445706329344 41.120994219991,15.819485626219 41.120994219991,15.819485626219 40.855620164394))
#    polygon2 = POLYGON((16.349232635497 40.791189284951,16.909535369872 40.791189284951,16.909535369872 41.131338714384,16.349232635497 41.131338714384,16.349232635497 40.791189284951))
#    polygon3 = POLYGON((17.349232635497 40.791189284951,16.909535369872 40.791189284951,16.909535369872 41.131338714384,16.349232635497 41.131338714384,17.349232635497 40.791189284951))
#    
#    [Platforms]
#
#    platform1 = Sentinel-1
#    platform2 = S-1
#    platform3 = ANY
#
#    [Types]
#    
#    type1 = SLC
#    type2 = SLC
#    type3 = ANY
#    
#    [Directions]
#    
#    direction1 = Descending
#    direction2 = Ascending
#    direction3 = ANY
#
#    [Directories]
#
#    directory1 = /alternative/path
#    directory2 = $HOME/
#    
#    [Authentication]
#    username = XXXXXXXX
#    password = YYYYYYY
#
# This file can be stored as /usr/local/etc/scihub.cfg or $HOME/.scihub.cfg or
# splitted among them, as more convenient. You can additionally pass its name as a
# command line argument.
#    

import pycurl
import urllib.request, urllib.parse, urllib.error
import sys
import getopt
import os.path
import configparser 
import sqlite3 as sqlite
import xml.etree.ElementTree as et
from io import BytesIO
from osgeo import ogr
import shapely.wkt
import zipfile
import re
import time
import magic
import dateutil.parser
from pathlib import Path

def usage():
    print('''usage: %s [-b date|-c|-d|-D path|-L path|-C path|-f|-h|-k|-l|-m|-v|-o|-r|-n|-t|-R|-F|-M int |-T int]''' % sys.argv[0])

def help():
    print('''
usage: %s [-b date|-c|-d|-D path|-f|-h|-k|-l|-m|-v|-L path|-C path|-o|-r|-t|-R]
          [--create|--download|--configuration=path|--data=path|--force|--help|
           --kml|--list|--manifest|--verbose|--products=path|--overwrite|--forever|
           --forevertime=seconds|--resume|--retrytime=seconds|--test|--refresh]
    -b --begin=<date> begin date to consider for products
    -c --create create db only
    -d --download download data .zip file
    -D --data=<path> name of SQLite database to use
    -C --configuration= <path> configuration file to use
    -f --force force
    -h --help this help
    -k --kml create KML skeleton addon files
    -l --list output XML list of entries
    -m --manifest download manifest files
    -v --verbose run verbosely
    -L --products=<path> output products names to file
    -o --overwrite overwrite data .zip/kml/manifest file even if it exists
    -r --resume try using resume to continue download
    -M --retrytime=<int> time in secs of waiting
    -n --noretry do not retry after a download failure
    -t --test test ZIP file at check time
    -R --refresh download missing/invalid/corrupted stuff on the basis of current db status
    -F --forever loop forever to download continuously images
    -T --forevertime=<time> loop time of waiting

An ESA SCIHUB username and password profile is required and read from a
scihub configuration file, such as:

    [Autentication]
    username = <user>[@realm]
    password = <xxxx>

Note that different realms can be used if the user is able to access not only
the main SciHub server but any other regional mirror or Collaborative Ground
Segment. If not specified, the main ESA one will be used.
''' % sys.argv[0])

def testzip(filename):
    try:
        z = zipfile.ZipFile(filename)
        z.testzip()
        return True
    except:
        return False

def isodate(date):
    iso = re.search('([0-9]{4}-[0-9]{2}-[0-9]{2})[T ]([0-9]{2}:[0-9]{2}:[0-9]{2})(\.[0-9]+)?Z?',date)
    return iso.group(1) + ' ' + iso.group(2)

def norm_platform(val):
    s1 = re.compile('[sS](entinel)?[-_]?1',re.IGNORECASE)
    s2 = re.compile('[sS](entinel)?[-_]?2',re.IGNORECASE)
    a = re.compile('any',re.IGNORECASE)
    if s1.match(val):
        return 'Sentinel-1'
    if s2.match(val):
        return 'Sentinel-2'
    if a.match(val):
        return 'ANY'
    raise ValueError("Invalid platform '%s'" % val)

def norm_direction(val):
    asc = re.compile('asc(ending)?',re.IGNORECASE)
    desc = re.compile('desc(ending)?',re.IGNORECASE)
    a = re.compile('any',re.IGNORECASE)
    if asc.match(val):
        return 'Ascending'
    if desc.match(val):
        return 'Descending'
    if a.match(val):
        return 'ANY'
    raise ValueError("Invalid direction '%s'" % val)

def norm_type(val):
    grd = re.compile('^GRD(H)?$',re.IGNORECASE)
    slc = re.compile('^SLC$',re.IGNORECASE)
    msl2 = re.compile('^S2MSI2A|MSIL2$',re.IGNORECASE)
    msl1 = re.compile('^S2MSI1C|MSIL1$',re.IGNORECASE)
    a = re.compile('any',re.IGNORECASE)
    if grd.match(val):
        return 'GRD'
    if slc.match(val):
        return 'SLC'
    if msl2.match(val):
        return 'S2MSI2A'
#        return 'S2MSI2Ap'
    if msl1.match(val):
        return 'S2MSI1C'
    if a.match(val):
        return 'ANY'
    raise ValueError("Invalid type '%s'" % val)

def norm_dir(val):
    return os.path.abspath(os.path.expandvars(val))

def say(*args):
    if verbose:
        print(' '.join(map(str, args)))

realms = {
    'apihub.esa.int' : {
        'searchbase' : 'https://scihub.copernicus.eu/apihub/search',
        'servicebase' : 'https://scihub.copernicus.eu/apihub/odata/v1'
    },
    'esa.int' : {
        'searchbase' : 'https://scihub.copernicus.eu/dhus/search',
        'servicebase' : 'https://scihub.copernicus.eu/dhus/odata/v1'
    },
    'asi.it' : {
        'searchbase' : 'http://collaborative.mt.asi.it/search',
        'servicebase' : 'http://collaborative.mt.asi.it/odata/v1'
    },
    'fmi.fi' : {
        'searchbase' : 'https://finhub.nsdc.fmi.fi/search',
        'servicebase' : 'https://finhub.nsdc.fmi.fi/odata/v1'
    },
    'noa.gr' : {
        'searchbase' : 'https://sentinels.space.noa.gr/dhus/search',
        'servicebase' : 'https://sentinels.space.noa.gr/dhus/odata/v1'
    },
    'colhub1' : {
        'searchbase' : 'https://colhub.copernicus.eu/dhus/search',
        'servicebase' : 'https://colhub.copernicus.eu/dhus/odata/v1'
    },
    'colhub2' : {
        'searchbase' : 'https://colhub2.copernicus.eu/dhus/search',
        'servicebase' : 'https://colhub2.copernicus.eu/dhus/odata/v1'
    },
    'inthub' : {
        'searchbase' : 'https://inthub.copernicus.eu/dhus/search',
        'servicebase' : 'https://inthub.copernicus.eu/dhus/odata/v1'
    },
}
searchbase = realms['apihub.esa.int']['searchbase']
servicebase = realms['apihub.esa.int']['servicebase']

products = []

data_download = False
manifest_download = False
output_list = False
verbose = False
kml = False
force = False
create_db = False
db_file = 'scihub.sqlite'
list_products = False
overwrite = False
configuration_file = '/usr/local/etc/scihub.cfg'
resume = False
test = False
refresh = False
forever = False
retry = True
begin_date = '2014-01-01'

default_direction = 'Ascending'
default_platform = 'Sentinel-1'
default_type = 'GRD'
default_ccp = 5
default_directory = os.path.abspath('.')
waiting_time = 28800
retrying_time = 300

try:
    m = magic.open(magic.MAGIC_MIME_TYPE)
    m.load()
except AttributeError as e:
    m = magic.Magic(mime=True)
    m.file = m.from_file

try:
    opts, args = getopt.getopt(sys.argv[1:],'b:cvfdhmklD:L:C:orM:tRFT:n',
            ['begin=','create','verbose','force','download','help','manifest','kml',
                'list','data=','products=','configuration=','overwrite',
                'resume','test','refresh', 'forever', 'forevertime=',
                'noretry', 'retrytime='])
except getopt.GetoptError:
    usage()
    sys.exit(3)

for opt, arg in opts:
    if opt in ['-m','--manifest']:
        manifest_download = True
    if opt in ['-b','--begin']:
        d = dateutil.parser.parse(arg)
        begin_date = '%s-%s-%s' % (d.year,d.month,d.day)
    if opt in ['-c','--create']:
        create_db = True
    if opt in ['-d','--download']:
        data_download = True
    if opt in ['-v','--verbose']:
        verbose = True
    if opt in ['-k','--kml']:
        kml = True
    if opt in ['-l','--list']:
        output_list = True
    if opt in ['-f','--force']:
        force = True
    if opt in ['-D','--data']:
        db_file = arg
    if opt in ['-L','--products']:
        list_products = True
        productsfile = arg
    if opt in ['-C','--configuration']:
        configuration_file = arg
    if opt in ['-o','--overwrite']:
        overwrite = True
    if opt in ['-r','--resume']:
        resume = True
    if opt in ['-n','--noretry']:
        retry = False
    if opt in ['-M','--retrytime']:
        resume = True
        retrying_time = int(arg)
    if opt in ['-t','--test']:
        test = True
    if opt in ['-R','--refresh']:
        refresh = True
    if opt in ['-F','--forever']:
        forever = True
    if opt in ['-T','--forevertime']:
        forever = True
        waiting_time = int(arg)
    if opt in ['-h','--help']:
        help()
        sys.exit(5)

try:
    db = sqlite.connect(db_file)
except sqlite.Error as e:
    print('Error %s:' % e.args[0])
    sys.exit(1)

if refresh:
    force = True

if create_db:
    cur = db.cursor()
    cur.executescript('''DROP TABLE IF EXISTS products;
            CREATE TABLE products(id integer primary key, 
                hash text, name text, idate text, bdate text, edate text, 
                ptype text, direction text, orbitno integer, 
                relorbitno integer, footprint text, platform text,
                footprint_r1 text, centroid_r1 text, outdir text);
            CREATE UNIQUE INDEX h ON products(hash);
            CREATE INDEX id ON products(idate);
            CREATE INDEX bd ON products(bdate);
            CREATE INDEX ed ON products(edate);
            CREATE INDEX dir ON products(direction);
            CREATE INDEX t ON products(ptype);
            CREATE INDEX orbno ON products(orbitno);
            CREATE INDEX p ON products(platform);
            CREATE INDEX rorbno ON products(relorbitno);
            CREATE INDEX fpr1 ON products(footprint_r1);
            CREATE INDEX c1 ON products(centroid_r1);
            CREATE INDEX od ON products(outdir);
            ''')
    db.commit()
    db.close()
    say("Database created")
    sys.exit(0)

auth = ''

try:
    config = configparser.ConfigParser()
    config.read([configuration_file,os.path.expanduser('~/.scihub.cfg')])

    username = re.search('([^@]+)@?(.*)',config.get('Authentication','username'))
    user = username.group(1)
    realm = username.group(2)
    if realm:
        try:
            searchbase = realms[realm]['searchbase']
            servicebase = realms[realm]['servicebase']
        except KeyError:
            print('Realm not found: %s' % realm)
            sys.exit(6)
    password = config.get('Authentication','password')
    auth = user + ':' + password
except configparser.Error as e:
    print('Error parsing configuration file: %s' % e)
    sys.exit(4)

general_platform = None
general_type = None
general_direction = None
general_ccperc = None
general_directory = None
try:
    general_platform = norm_platform(config.get('Global','platform'))
    general_type = norm_type(config.get('Global','type'))
    general_direction = norm_direction(config.get('Global','direction'))
    general_ccperc = config.get('Global','cloudcoverpercentage')
    general_directory = norm_dir(config.get('Global','directory'))
except configparser.Error as e:
    pass
if general_platform:
    default_platform = general_platform
if general_direction:
    default_direction = general_direction
if general_type:
    default_type = general_type
if general_ccperc:
    default_ccp = general_ccperc
if general_directory:
    default_directory = general_directory


polygons = []
types = []
directions = []
platforms = []
directories = []

polygons_items = config.items('Polygons')
for key, polygon in polygons_items:
    polygons.append(polygon)

try:
    types_items = config.items('Types')
    for key, typ in types_items:
        types.append(typ)
    directions_items = config.items('Directions')
    for key, direction in directions_items:
        directions.append(direction)
    platform_items = config.items('Platforms')
    for key, platform in platform_items:
        platforms.append(platform)
    directory_items = config.items('Directories')
    for key, directory in directory_items:
        if directory:
            directories.append(directory)
        else:
            directories.append(os.path.abspath('.'))
except:
    pass



for i in range(len(polygons)):
    try:
        platforms[i] = norm_platform(platforms[i])
    except IndexError:
        platforms.append(default_platform)
    try:
        types[i] = norm_type(types[i])
    except IndexError:
        types.append(default_type)
    try:
        directions[i] = norm_direction(directions[i])
    except IndexError:
        directions.append(default_direction)
    try:
        directories[i] = norm_dir(directories[i])
    except IndexError:
        directories.append(default_directory)

        say('Polygon: %s, %s, %s, %s, %s' % \
            (polygons[i], platforms[i], types[i], directions[i], directories[i]))

if not len(auth):
    print('Missing ESA SCIHUB authentication information')
    sys.exit(7)


do = True

while do:


    if not refresh:

        cur = db.cursor()
        cur.execute('''select date(idate) as d from products order by d desc limit 1''')
        last = cur.fetchone()
        if last is None or force:
            last = []
            last.append(begin_date)

        say('Latest ingestion date considered: %s' % last[0])

        refdate = last[0] + 'T00:00:00.000Z'

        criteria = []
        for i in range(len(polygons)):
            criteria.append({'platform':platforms[i] , \
                             'type':types[i], 'direction':directions[i], \
                             'polygon':polygons[i]})

        params = []
        for criterium in criteria:
            if not criterium['platform'] in ['ANY',]:
                str_platform = " AND platformname:%s " % criterium['platform']
            else:
                str_platform = ""

            if not criterium['direction'] in ['ANY',]:
                str_direction = " AND orbitdirection:%s " % criterium['direction']
            else:
                str_direction = ""

            if not criterium['type'] in ['ANY',]:
                str_type = " AND producttype:%s " % criterium['type']
            else:
                str_type = ""

            params.append({'q': '''ingestiondate:[%s TO NOW]%s%s%sAND footprint:"Intersects(%s)"''' % \
                    (refdate, str_platform, str_type, str_direction, criterium['polygon']), })

        # urls need encoding due to complexity of arguments

        urls = []
        for param in params:
            urls.append(searchbase + '?' + urllib.parse.urlencode(param))

        for index, url in enumerate(urls):
            page = 0
            outdir = directories[index]
            while True: 
                stop = True
                page_url = url + '&' + urllib.parse.urlencode({'rows': 100,'start' : page*100})
                buffer = BytesIO()
                c = pycurl.Curl()
                c.setopt(c.URL,str(page_url))
                c.setopt(c.USERPWD,auth)
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(c.SSL_VERIFYPEER, False)
                c.setopt(c.WRITEFUNCTION,buffer.write)
                say("get %s..." % page_url)
                c.perform()
                c.close()

                body = buffer.getvalue()
                if output_list:
                    print(body + '\n')
                try:
                    root = et.fromstring(body)
                except et.ParseError:
                    print(body)
                    sys.exit(2)

                for entry in root.iter('{http://www.w3.org/2005/Atom}entry'):
                    id = entry.find('{http://www.w3.org/2005/Atom}id').text
                    title = entry.find('{http://www.w3.org/2005/Atom}title').text
                    footprint = ''
                    orbitdirection = ''
                    producttype = ''
                    beginposition = ''
                    endposition = ''
                    ingdate = ''
                    for string in entry.iter('{http://www.w3.org/2005/Atom}str'):
                        if 'name' in string.attrib:
                            if string.attrib['name'] == 'footprint':
                                footprint = string.text
                            if string.attrib['name'] == 'orbitdirection':
                                orbitdirection = string.text
                            if string.attrib['name'] == 'producttype':
                                producttype = string.text
                            if string.attrib['name'] == 'platformname':
                                platform = string.text
                    for string in entry.iter('{http://www.w3.org/2005/Atom}date'):
                        if 'name' in string.attrib:
                            if string.attrib['name'] == 'ingestiondate':
                                ingdate = string.text
                            if string.attrib['name'] == 'beginposition':
                                beginposition = string.text
                            if string.attrib['name'] == 'endposition':
                                endposition = string.text
                    for string in entry.iter('{http://www.w3.org/2005/Atom}int'):
                        if 'name' in string.attrib:
                            if string.attrib['name'] == 'orbitnumber':
                                orbitno = string.text
                            if string.attrib['name'] == 'relativeorbitnumber':
                                relorbitno = string.text
                    products.append([id,title,ingdate,footprint,beginposition,endposition,orbitdirection,producttype,orbitno,relorbitno,platform,outdir,])
                    # still products available, guess we can query again...
                    stop = False
                    say(products[-1])
                page = page + 1
                if stop:
                    break
    else:

        say("Refreshing from database contents...")
        cur = db.cursor()
        for entry in cur.execute('''SELECT * FROM products order by idate desc'''):
            products.append([entry[1],entry[2],entry[3],entry[10],entry[4], \
                entry[5],entry[7],entry[6],entry[8],entry[9],entry[11],entry[14],])
            say(products[-1])

    cur = db.cursor()

    if list_products:
        pf = open(productsfile,'w')

    for product in products:
        uniqid = product[0]
        sub = uniqid[0:4]
        name = product[1]
        idate = isodate(product[2])
        footprint = product[3]
        bdate = isodate(product[4])
        edate = isodate(product[5])
        direction = product[6]
        ptype = product[7]
        orbitno = product[8]
        relorbitno = product[9]
        platform = product[10]
        outdir = product[11]
        cur.execute('''SELECT COUNT(*) FROM products WHERE hash=?''',(uniqid,))
        row = cur.fetchone()

        Path(os.path.join(outdir,sub)).mkdir(parents=True, exists_ok=True)

        if list_products:
            pf.write('%s\n' % name)

        if not row[0] or force:
            if manifest_download:
                manifest = "%s/Products('%s')/Nodes('%s.SAFE')/Nodes('manifest.safe')/$value" % (servicebase,uniqid,name)
                filename = "%s.manifest" % name
                if overwrite or not os.path.exists(os.path.join(outdir, sub, filename)):
                    say("downloading %s manifest file..." % name)
                    with open(os.path.join(outdir, sub, filename), 'wb') as f:
                        c = pycurl.Curl()
                        c.setopt(c.URL,manifest)
                        c.setopt(c.FOLLOWLOCATION, True)
                        c.setopt(c.SSL_VERIFYPEER, False)
                        c.setopt(c.USERPWD,auth)
                        c.setopt(c.WRITEFUNCTION,f.write)
                        c.perform()
                        c.close()
                else:
                    say("skipping existing %s manifest file" % name)

            if data_download:
                data = "%s/Products('%s')/$value" % (servicebase, uniqid)
                filename = "%s.zip" % name
                fullname = os.path.join(outdir, sub, filename)
                if overwrite or not os.path.exists(fullname) or not zipfile.is_zipfile(fullname) or \
                            (test and not testzip(fullname)):
                    say("downloading %s data file..." % name)

                    loop = True
                    while loop:
                        if not overwrite and resume and os.path.exists(fullname) and m.file(fullname) == 'application/zip' :
                            counter = os.path.getsize(fullname)
                            mode = 'ab'
                            say("resuming download starting from byte %d" % counter)
                        else:
                            counter = 0
                            mode = 'wb'
                        with open(fullname, mode) as f:
                            c = pycurl.Curl()
                            c.setopt(c.URL,data)
                            c.setopt(c.FOLLOWLOCATION, True)
                            c.setopt(c.SSL_VERIFYPEER, False)
                            c.setopt(c.USERPWD,auth)
                            c.setopt(c.WRITEFUNCTION,f.write)
                            c.setopt(c.RESUME_FROM_LARGE,counter)
                            c.setopt(c.FAILONERROR,True)
                            c.setopt(c.LOW_SPEED_LIMIT,100)
                            c.setopt(c.LOW_SPEED_TIME,300)
                            try:
                                c.perform()
                                loop = False
                            except:
                                if retry: 
                                    loop = True
                                    say("download failed, restarting in %d seconds..." % retrying_time)
                                    time.sleep(retrying_time)
                            c.close()
                            if m.file(fullname) != 'application/zip':
                                if retry:
                                    loop = True
                                    say("downloaded file invalid, restarting in %d seconds..." % retrying_time)
                                    time.sleep(retrying_time)
                else:
                    say("skipping existing file %s" % filename)


            if kml:
                poly = ogr.CreateGeometryFromWkt(footprint)
                style = '''<Style
id="ballon-style"><BalloonStyle><text><![CDATA[
Name = $[Name]
IngestionDate = $[IngestionDate]
BeginDate = $[BeginDate]
EndDate = $[EndDate]
ProductType = $[ProductType]
OrbitDirection = $[OrbitDirection]
OrbitNumber = $[OrbitNumber]
RelativeOrbitNumber = $[RelativeOrbitNumber]
Platform = $[PlatformName]
]]>
</text></BalloonStyle></Style>
'''
                extdata = '''<ExtendedData>
<Data name="Name"><value>%s</value></Data>
<Data name="IngestionDate"><value>%s</value></Data>
<Data name="BeginDate"><value>%s</value></Data>
<Data name="EndDate"><value>%s</value></Data>
<Data name="ProductType"><value>%s</value></Data>
<Data name="OrbitDirection"><value>%s</value></Data>
<Data name="OrbitNumber"><value>%s</value></Data>
<Data name="RelativeOrbitNumber"><value>%s</value></Data>
<Data name="PlatformName"><value>%s</value></Data>
</ExtendedData> ''' % (name,idate,bdate,edate,ptype,direction,orbitno,relorbitno,platform)
                buff = '''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>%s<Placemark><name>%s</name><StyleUrl>#ballon-style</StyleUrl>%s%s</Placemark></Document></kml>''' % (style,name,extdata,poly.ExportToKML())
                if overwrite or not os.path.exists(os.path.join(outdir, sub, name+'.kml')):
                    kmlfile = open(os.path.join(outdir, sub, name)+'.kml','w')
                    kmlfile.write(buff)
                    kmlfile.close()
                    say("KML file %s.kml created" % name)
                else:
                    say("KML file %s.kml skipped" % name)

            if not refresh:
                simple = shapely.wkt.loads(footprint)
                footprint_r1 = shapely.wkt.dumps(simple,rounding_precision=1)
                centroid_r1 = shapely.wkt.dumps(simple.centroid,rounding_precision=1)
                cur.execute('''INSERT OR REPLACE INTO products 
                        (id,hash,name,idate,bdate,edate,ptype,direction,orbitno,relorbitno,footprint,platform,footprint_r1,centroid_r1,outdir) 
                        VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', 
                        (uniqid,name,idate,bdate,edate,ptype,direction,orbitno,relorbitno,footprint,platform,footprint_r1,centroid_r1,outdir))
                db.commit()
        else:
            say("skipping %s" % name)

    if list_products:
        pf.close()

    if not forever:
        do = False
    else:
        say("Waiting %d seconds" % waiting_time)
        db.close()
        time.sleep(waiting_time)
        db = sqlite.connect(db_file)

db.close()
sys.exit(0)

