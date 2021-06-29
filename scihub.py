#!/usr/bin/env python3
#
#   Copyright (C) 2015-2021 Francesco P. Lovergine <francesco.lovergine@cnr.it>
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

import sys
import getopt
import os
import os.path
from osgeo import ogr
import shapely.wkt
import zipfile
import re
import time
import dateutil.parser
import spatialite
from pathlib import Path
import datetime

from sentinelsat.sentinel import SentinelAPI
from sentinelsat.exceptions import *
from ruamel.yaml import YAML
import tenacity
from collections import defaultdict

realms = {
    'apihub.esa.int' : 'https://apihub.copernicus.eu/apihub/',
    'esa.int' : 'https://scihub.copernicus.eu/dhus/',
    'fmi.fi' : 'https://finhub.nsdc.fmi.fi/',
    'noa.gr' : 'https://sentinels.space.noa.gr/dhus/',
    'colhub1' : 'https://colhub.copernicus.eu/dhus/',
    'colhub2' : 'https://colhub2.copernicus.eu/dhus/',
    'inthub' : 'https://inthub.copernicus.eu/dhus/'
}
servicebase = realms['apihub.esa.int']

products = []

data_download = False
output_list = False
verbose = False
kml = False
force = False
create_db = False
db_file = 'scihub.splite'
list_products = False
overwrite = False
configuration_file = '/usr/local/etc/scihub.yml'
user_configuration_file = '~/.scihub.yml'
test = False
refresh = False
forever = False
begin_date = '2014-01-01'
end_date = None
empty_queue = False
inject_products = False
prod_n_dest = list()

default_direction = 'Ascending'
default_platform = 'Sentinel-1'
default_type = 'GRD'
default_ccp = 5
default_directory = os.path.abspath('.')
waiting_time = 28800

def usage():
    print('''usage: %s [-b date|-e date|-c|-d|-D path|-L path|-C path|-U path|-I path|-f|-h|-k|-l|-v|-o|-t|-Q|-R|-F|-T int]''' % sys.argv[0])

def help():
    print('''
usage: %s [-b date|-e date|-c|-d|-D path|-f|-h|-k|-l|-m|-v|-L path|-C path|-U path|-I path:destination|-o|-r|-t|-R|-Q]
          [--create|--download|--configuration=path|--inject=path:destination|--data=path|--force|--help|
           --kml|--list|--verbose|--products=path|--overwrite|--forever|
           --forevertime=seconds|--test|--refresh|--queue]
    -b --begin=<date> begin date to consider for products
    -b --end=<date> end date to consider for products
    -c --create create db only
    -d --download download data .zip file
    -D --data=<path> name of Spatialite database to use
    -C --configuration=<path> YAML configuration file to use
    -U --user-configuration=<path> YAML user's configuration file to use
    -I --inject=<path:destination> inject existing product and link to destination tree
    -f --force force
    -h --help this help
    -k --kml create KML skeleton addon files
    -l --list output list of entries
    -v --verbose run verbosely
    -L --products=<path> output products names to file
    -o --overwrite overwrite data .zip/kml file even if it exists
    -t --test test ZIP file at check time
    -R --refresh download missing/invalid/corrupted stuff on the basis of current db status
    -F --forever loop forever to download continuously images
    -T --forevertime=<time> loop time of waiting
    -Q --queue download pending LTA products

A Copenicus Open Data Hub username and password profile is required and read from a
scihub configuration YAML file, such as:

username: <user>[@realm]
password: <xxxx>

Note that different realms can be used if the user is able to access not only
the main SciHub server but any other regional mirror or Collaborative Ground
Segment. If not specified, the main one (APIHUB) will be used.
''' % sys.argv[0])

def testzip(filename):
    try:
        z = zipfile.ZipFile(filename)
        z.testzip()
        return True
    except:
        return False

def isodate(date):
    if isinstance(date,datetime.date):
        date = date.strftime("%Y-%m-%d %H:%M:%S")
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

def create_schema(db):
    cur = db.cursor()
    cur.executescript('''
            BEGIN TRANSACTION;
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
            CREATE INDEX od ON products(outdir);
            SELECT InitSpatialMetaData();
            SELECT AddGeometryColumn( 'products', '_footprint', 4326, 'MULTIPOLYGON', 'XY');
            SELECT CreateSpatialIndex('products', '_footprint');
            CREATE TABLE queue(hash text, name text, outdir text, status text);
            CREATE INDEX qs ON queue(status);
            CREATE UNIQUE INDEX qh ON queue(hash);
            PRAGMA journal_mode=WAL;
            COMMIT;
            ''')
    db.close()
    say("Database created")

def create_kml(outdir, sub, name, footprint):
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

@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(3600))
def download_all(*args, **kwargs):
    api = SentinelAPI(user, password, servicebase)
    return api.download_all(*args, **kwargs)

def download_queue(db):
    cur = db.cursor()
    ids = defaultdict(list)
    names = defaultdict(list)
    dirs = defaultdict(list)
    api = SentinelAPI(user, password, servicebase)
    for entry in cur.execute('''SELECT hash, name, outdir, substr(hash,1,4), status from queue where status != "pending" '''):
        d = os.path.join(entry[2],entry[3])
        ids[d].append(entry[0])
        names[d].append(entry[1])
        dirs[d].append(entry[2])
    for dir in ids.keys():
        say("dir: %s" % dir)
        say(ids[dir])
        cur.execute('''UPDATE queue SET status="pending" WHERE hash=?''', ids[dir])
        db.commit()
        try:
            downloaded, triggered, failed = api.download_all(ids[dir], directory_path=dir, n_concurrent_dl=4, max_attempts=4, lta_retry_delay=30)
            for hash in downloaded.keys():
                cur.execute('''DELETE FROM queue WHERE hash=?''', (hash,))
            for hash in triggered.keys():
                cur.execute('''UPDATE queue SET status="requested" WHERE hash=?''', (hash,))
            for hash in failed.keys():
                cur.execute('''UPDATE queue SET status="queued" WHERE hash=?''', (hash,))
        except Exception as e:
            cur.execute('''UPDATE queue SET status="queued" WHERE hash=?''', ids[dir])
            say(e)
            pass
    db.close()

def inject_prods(db, prods):
    api = SentinelAPI(user, password, servicebase)
    cur = db.cursor()
    for str in prods:
        prod = str.split(':', 1)
        name = prod[0]
        dir = prod[1]
        if os.path.exists(name + '.zip') and os.path.isdir(dir):
            filename = os.path.basename(name)
            say("Injecting product %s in %s" % (filename, dir))
            args = { 'filename': filename + '*', }
            results = api.query( area=None, date=None, **args )
            if results is not None:
                print(results)
                for product, metadata in results.items():
                    uniqid = product
                    sub = product[0:4]
                    filename = metadata['filename'][:-5]
                    idate = metadata['ingestiondate']
                    bdate = metadata['beginposition']
                    edate = metadata['endposition']
                    ptype = metadata['producttype']
                    direction = metadata['orbitdirection']
                    orb = metadata['orbitnumber']
                    relorb = metadata['relativeorbitnumber']
                    footprint = metadata['footprint']
                    platform = metadata['platformname']
                    say('''
                    product: %s
                    filename: %s
                    dir: %s
                    sub: %s
                    idate: %s
                    bdate: %s
                    edate: %s
                    type: %s
                    direction: %s
                    orbit: %s
                    relorbit: %s
                    footprint: %s
                    platform: %s''' % (product, filename, dir, sub, idate, bdate, edate, ptype, direction, orb, relorb, footprint, platform) )
                    simple = shapely.wkt.loads(footprint)
                    footprint_r1 = shapely.wkt.dumps(simple,rounding_precision=1)
                    centroid_r1 = shapely.wkt.dumps(simple.centroid,rounding_precision=1)
                    Path(os.path.join(dir, sub)).mkdir(parents=True, exist_ok=True)
                    os.link(name+'.zip', os.path.join(dir, sub, filename+'.zip'))
                    if os.path.exists(name+'.kml'):
                        os.link(name+'.kml', os.path.join(dir, sub, filename+'.kml'))
                    if os.path.exists(name+'.manifest'):
                        os.link(name+'.manifest', os.path.join(dir, sub, filename+'.manifest'))
                    cur.execute('''INSERT OR REPLACE INTO products 
                            (id,hash,name,idate,bdate,edate,ptype,direction,orbitno,relorbitno,footprint,platform,footprint_r1,centroid_r1,outdir,_footprint) 
                            VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CastToMultipolygon(ST_GeomFromText(?,4326)))''', 
                            (uniqid, filename, idate, bdate, edate, ptype, direction, orb, relorb, footprint, platform, footprint_r1, centroid_r1, dir, footprint))
                    say('Product %s inserted in ')
            else:
                say('Product %s not found' % (filename, ))
        else:
            say("File %s not found, skipped" % (name+'.zip',))
            

#
# Parsing command line arguments
#

try:
    opts, args = getopt.getopt(sys.argv[1:],'b:e:cvfdhklD:L:C:U:otRFT:Q',
            ['begin=','end=','create','verbose','force','download','help','kml',
                'list','data=','products=','configuration=','user-configuration=','inject=','overwrite',
                'test','refresh', 'forever', 'forevertime=','queue' ])
except getopt.GetoptError:
    usage()
    sys.exit(3)

for opt, arg in opts:
    if opt in ['-b','--begin']:
        d = dateutil.parser.parse(arg)
        begin_date = '%04d-%02d-%02d' % (d.year,d.month,d.day)
    if opt in ['-e','--end']:
        d = dateutil.parser.parse(arg)
        end_date = '%04d-%02d-%02d' % (d.year,d.month,d.day)
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
    if opt in ['-U','--user-configuration']:
        user_configuration_file = arg
    if opt in ['-I','--inject']:
        inject_products = True
        prod_n_dest.append(arg)
    if opt in ['-o','--overwrite']:
        overwrite = True
    if opt in ['-t','--test']:
        test = True
    if opt in ['-R','--refresh']:
        refresh = True
    if opt in ['-F','--forever']:
        forever = True
    if opt in ['-T','--forevertime']:
        forever = True
        waiting_time = int(arg)
    if opt in ['-Q','--queue']:
        empty_queue = True
    if opt in ['-h','--help']:
        help()
        sys.exit(5)

try:
    db = spatialite.connect(db_file, isolation_level=None)
except spatialite.Error as e:
    print('Error %s:' % e.args[0])
    sys.exit(1)

if refresh:
    force = True

if create_db:
    create_schema(db)
    sys.exit(0)

auth = ''

# Read YAML configs

try:
    yaml = YAML()
    config = yaml.load(Path(configuration_file))
    user_config = yaml.load(Path(os.path.expanduser(user_configuration_file)))

    username = re.search('([^@]+)@?(.*)', user_config['username'])
    user = username.group(1)
    realm = username.group(2)
    if realm:
        try:
            servicebase = realms[realm]
        except KeyError:
            print('Realm not found: %s' % realm)
            sys.exit(6)
    password = user_config['password']

except Exception as e:
    print(e)
    sys.exit(4)

if not len(user) or not len(password):
    print('Missing Copernicus Open Data Hub credentials')
    sys.exit(7)

if empty_queue:
    download_queue(db)
    sys.exit(0)

if inject_products:
    inject_prods(db, prod_n_dest)
    sys.exit(0)

general_platform = None
general_type = None
general_direction = None
general_ccperc = None
general_directory = None

polygons = []
types = []
directions = []
platforms = []
directories = []
ccp = []
queue = []

for c in config:
    try:
        general_platform = norm_platform(config[c]['platform'])
        general_type = norm_type(config[c]['type'])
        general_direction = norm_direction(config[c]['direction'])
        general_ccperc = config[c]['cloudcoverpercentage']
        general_directory = norm_dir(config[c]['directory'])
    except Exception as e:
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

    say('''
    user: %s
    password: %s
    servicebase: %s
    default_platform: %s 
    default_direction: %s
    default_type: %s
    default_ccp: %s
    default_directory: %s
    ''' % (user, '<hidden>', servicebase, default_platform, default_direction, default_type, default_ccp, default_directory))

    for aoi in config[c]['items']:

        polygons.append(aoi['polygon'])

        try:
            directories.append(norm_dir(aoi['directory']))
        except:
            directories.append(default_directory)

        try:
            types.append(norm_type(aoi['type']))
        except:
            types.append(default_type)

        try:
            directions.append(norm_direction(aoi['direction']))
        except:
            directions.append(default_direction)
         
        try:
            ccp.append(aoi['cloudcovepercentage'])
        except:
            ccp.append(default_ccp)

        try:
            platforms.append(norm_platform(aoi['platform']))
        except:
            platforms.append(default_platform)

# Now searching for all defined polygons

api = SentinelAPI(user, password, servicebase)

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

        for index, polygon in enumerate(polygons):
            outdir = directories[index]
            if end_date is None:
                end_date = 'NOW'
            else:
                if end_date != 'NOW':
                    end_date = end_date + 'T23:59:59.000Z'
            args = {
                'ingestiondate': (refdate, end_date), 
                'platformname': platforms[index], 
                'producttype': types[index],
            }
            if directions[index] in ['Ascending', 'Descending']:
                args['orbitdirection'] = directions[index]
            if platforms[index] in ['Sentinel-2']:
                args['cloudcoverpercentage'] = (0, ccp[index])

            results = api.query(polygon, date=None, **args)
            if results is not None:
                for product, metadata in results.items():
                    sub = product[0:4]
                    filename = metadata['filename'][:-5]
                    idate = metadata['ingestiondate']
                    bdate = metadata['beginposition']
                    edate = metadata['endposition']
                    ptype = metadata['producttype']
                    direction = metadata['orbitdirection']
                    orb = metadata['orbitnumber']
                    relorb = metadata['relativeorbitnumber']
                    footprint = metadata['footprint']
                    platform = metadata['platformname']
                    say('''
                    product: %s
                    filename: %s
                    dir: %s
                    sub: %s
                    idate: %s
                    bdate: %s
                    edate: %s
                    type: %s
                    direction: %s
                    orbit: %s
                    relorbit: %s
                    footprint: %s
                    platform: %s''' % (product, filename, outdir, sub, idate, bdate, edate, ptype, direction, orb, relorb, footprint, platform) )
                    products.append([product, filename, idate, footprint, bdate, edate, direction, ptype, orb, relorb, platform, outdir,])

    else:

        say("Refreshing from database contents...")
        cur = db.cursor()
        for entry in cur.execute('''SELECT hash,name,idate,footprint,bdate,edate,direction,ptype,orbitno,relorbitno,platform,outdir FROM products order by idate desc'''):
            products.append(entry)
            say(products[-1])

    cur = db.cursor()

    if output_list:
        for product in products:
            print(product)

    if list_products:
        pf = open(productsfile,'w')

#
# Now download products and/or create KML files
#
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

        if kml or data_download:
            Path(os.path.join(outdir, sub)).mkdir(parents=True, exist_ok=True)

        if list_products:
            pf.write('%s|%s\n' % (uniqid, name))

        if not row[0] or force:

            if data_download:
                filename = "%s.zip" % name
                fullname = os.path.join(outdir, sub, filename)
                if overwrite or not os.path.exists(fullname) or not zipfile.is_zipfile(fullname) or \
                            (test and not testzip(fullname)):
                    if api.is_online(uniqid):
                        say("downloading %s data file..." % name)
                        try:
                            api.download(id=uniqid, directory_path=os.path.join(outdir,sub))
                        except Exception as e:
                            say(e)
                            pass
                    else:
                        say("queuing %s data file..." % name )
                        try:
                            api.trigger_offline_retrieval(uniqid)
                            cur.execute('''INSERT OR REPLACE INTO queue (hash, name, outdir, status) VALUES (?,?,?,?)''', (uniqid, name, outdir, 'requested'))
                            say("Triggered data download")
                        except:
                            cur.execute('''INSERT OR REPLACE INTO queue (hash, name, outdir, status) VALUES (?,?,?,?)''', (uniqid, name, outdir,'queued'))
                            say("Cannot trigger data download")
                            pass

                else:
                    say("skipping existing file %s" % filename)

            if kml:
                create_kml(outdir, sub, name, footprint)

            if not refresh:
                simple = shapely.wkt.loads(footprint)
                footprint_r1 = shapely.wkt.dumps(simple,rounding_precision=1)
                centroid_r1 = shapely.wkt.dumps(simple.centroid,rounding_precision=1)
                cur.execute('''INSERT OR REPLACE INTO products 
                        (id,hash,name,idate,bdate,edate,ptype,direction,orbitno,relorbitno,footprint,platform,footprint_r1,centroid_r1,outdir,_footprint) 
                        VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CastToMultipolygon(ST_GeomFromText(?,4326)))''', 
                        (uniqid, name, idate, bdate, edate, ptype, direction, orbitno, relorbitno, footprint, platform, footprint_r1, centroid_r1, outdir, footprint))
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
        db = spatialite.connect(db_file, isolation_level=None)

db.close()
sys.exit(0)

