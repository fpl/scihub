#!/usr/bin/env python
#
#   Copyright (C) 2015 Francesco P. Lovergine <f.lovergine@ba.issia.cnr.it>
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
#    
#    [Types]
#    
#    type1 = SLC
#    type2 = SLC
#    
#    [Directions]
#    
#    direction1 = Descending
#    direction2 = Ascending
#    
#    [Authentication]
#    username = XXXXXXXX
#    password = YYYYYYY
#
# This file can be stored as /usr/local/etc/scihub.cfg or $HOME/.scihub.cfg or
# splitted among them, as more convenient.
#    

import pycurl
import urllib
import sys
import getopt
import os.path
import ConfigParser as configparser
import sqlite3 as sqlite
import xml.etree.ElementTree as et
from StringIO import StringIO
from osgeo import ogr
import shapely.wkt
import zipfile

def usage():
    print '''usage: %s [-c|-d|-D path|-C path|-f|-h|-k|-l|-m|-v|-o]''' % sys.argv[0]

def help():
    print '''
usage: %s [-c|-d|-D path|-f|-h|-k|-l|-m|-v|-L path]
    -c  create db only
    -d  download data .zip file
    -D <path> name of SQLite database to use
    -C <path> configuration file to use
    -f  force
    -h  this help
    -k  create KML skeleton addon file
    -l  output XML list of entries
    -m  download manifest file
    -v  run verbosely
    -L <path> output products names to file
    -o  overwrite data .zip file even if it exists

An ESA SCIHUB username and password profile is required and read from a
scihub configuration file, such as:

    [Autentication]
    username = <user>
    password = <xxxx>
''' % sys.argv[0]

def isodate(date):
    iso, ignore = date.replace('T',' ').replace('Z','')
    return iso[0:19]

searchbase = 'https://scihub.esa.int/dhus/search'
servicebase = 'https://scihub.esa.int/dhus/odata/v1'

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

try:
    opts, args = getopt.getopt(sys.argv[1:],'cvfdhmklD:L:C:')
except getopt.GetoptError:
    usage()
    sys.exit(3)

for opt, arg in opts:
    if opt == '-m':
        manifest_download = True
    if opt == '-c':
        create_db = True
    if opt == '-d':
        data_download = True
    if opt == '-v':
        verbose = True
    if opt == '-k':
        kml = True
    if opt == '-l':
        output_list = True
    if opt == '-f':
        force = True
    if opt == '-D':
        dbfile = arg
    if opt == '-L':
        list_products = True
        productsfile = arg
    if opt == '-C':
        configuration_file = arg
    if opt == '-o':
        overwrite = True
    if opt == '-h':
        help()
        sys.exit(5)

try:
    db = sqlite.connect(db_file)
except sqlite.Error, e:
    print 'Error %s:' % e.args[0]
    sys.exit(1)

if create_db:
    cur = db.cursor()
    cur.executescript('''DROP TABLE IF EXISTS products;
            CREATE TABLE products(id integer primary key, 
                hash text, name text, idate text, bdate text, edate text, 
                ptype text, direction text, orbitno integer, 
                relorbitno integer, footprint text, platform text,
                footprint_r1 text, centroid_r1 text);
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
            ''')
    db.commit()
    db.close()
    if verbose:
        print "Database created"
    sys.exit(0)

auth = ''

try:
    config = configparser.ConfigParser()
    config.read([configuration_file, os.path.expanduser('~/.scihub.cfg')])

    username = config.get('Authentication','username')
    password = config.get('Authentication','password')
    auth = username + ':' + password
    polygons = []
    polygons_items = config.items('Polygons')
    for key, polygon in polygons_items:
        polygons.append(polygon)
    types = []
    types_items = config.items('Types')
    for key, typ in types_items:
        types.append(typ)
    directions = []
    directions_items = config.items('Directions')
    for key, direction in directions_items:
        directions.append(direction)

    if len(types) != len(polygons) or len(directions) != len(polygons):
        print 'Incorrect number of polygons, types and direction in configuration file'
        sys.exit(6)

    if verbose:
        for i in range(len(polygons)):
            print 'Polygon: %s, %s, %s' % (polygons[i], types[i], directions[i])

except configparser.Error, e:
    print 'Error parsing configuration file: %s' % e
    sys.exit(4)

if not len(auth):
    print 'Missing ESA SCIHUB authentication information'
    sys.exit(7)

cur = db.cursor()

cur.execute('''select date(idate) as d from products order by d desc limit 1''')
last = cur.fetchone()
if last is None or force:
    last = []
    last.append('2014-01-01')

if verbose:
    print 'Latest ingestion date considered: %s' % last[0]

refdate = last[0] + 'T00:00:00.000Z'

criteria = []
for i in range(len(polygons)):
    criteria.append({'type':types[i], 'direction': directions[i], 'polygon':polygons[i]})

params = []
for criterium in criteria:
    params.append({'q': '''ingestiondate:[%s TO NOW] AND producttype:%s AND orbitdirection:%s AND footprint:"Intersects(%s)"''' % \
        (refdate, criterium['type'],criterium['direction'],criterium['polygon']), 'rows': '1000', 'start':'0'})

# urls need encoding due to complexity of arguments

urls = []
for param in params:
    urls.append(searchbase + '?' + urllib.urlencode(param))

for url in urls:
    buffer = StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL,str(url))
    c.setopt(c.USERPWD,auth)
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(c.SSL_VERIFYPEER, False)
    c.setopt(c.WRITEFUNCTION,buffer.write)
    if verbose:
        print "get %s..." % url
    c.perform()
    c.close()

    body = buffer.getvalue()
    if output_list:
        print body + '\n'
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
            if string.attrib.has_key('name'):
                if string.attrib['name'] == 'footprint':
                    footprint = string.text
                if string.attrib['name'] == 'orbitdirection':
                    orbitdirection = string.text
                if string.attrib['name'] == 'producttype':
                    producttype = string.text
                if string.attrib['name'] == 'platformname':
                    platform = string.text
        for string in entry.iter('{http://www.w3.org/2005/Atom}date'):
            if string.attrib.has_key('name'):
                if string.attrib['name'] == 'ingestiondate':
                    ingdate = string.text
                if string.attrib['name'] == 'beginposition':
                    beginposition = string.text
                if string.attrib['name'] == 'endposition':
                    endposition = string.text
        for string in entry.iter('{http://www.w3.org/2005/Atom}int'):
            if string.attrib.has_key('name'):
                if string.attrib['name'] == 'orbitnumber':
                    orbitno = string.text
                if string.attrib['name'] == 'relativeorbitnumber':
                    relorbitno = string.text
        products.append([id,title,ingdate,footprint,beginposition,endposition,orbitdirection,producttype,orbitno,relorbitno,platform])
        if verbose:
            print id, title, ingdate, footprint, beginposition, endposition, \
                    orbitdirection, producttype, orbitno, relorbitno, platform

cur = db.cursor()

if list_products:
    pf = open(productsfile,'w')

for product in products:
    uniqid = product[0]
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
    cur.execute('''SELECT COUNT(*) FROM products WHERE hash=?''',(uniqid,))
    row = cur.fetchone()

    if list_products:
        pf.write('%s\n' % name)

    if not row[0] or force:
        if manifest_download:
            manifest = "%s/Products('%s')/Nodes('%s.SAFE')/Nodes('manifest.safe')/$value" % (servicebase,uniqid,name)
            filename = "%s.manifest" % name
            if verbose:
                print "downloading %s manifest file..." % name
            with open(filename, 'wb') as f:
                c = pycurl.Curl()
                c.setopt(c.URL,manifest)
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(c.SSL_VERIFYPEER, False)
                c.setopt(c.USERPWD,auth)
                c.setopt(c.WRITEFUNCTION,f.write)
                c.perform()
                c.close()

        if data_download:
            data = "%s/Products('%s')/$value" % (servicebase, uniqid)
            filename = "%s.zip" % name
            if not os.path.exists(filename) or not zipfile.is_zipfile(filename) or overwrite:
                if verbose: 
                    print "downloading %s data file..." % name
                with open(filename, 'wb') as f:
                    c = pycurl.Curl()
                    c.setopt(c.URL,data)
                    c.setopt(c.FOLLOWLOCATION, True)
                    c.setopt(c.SSL_VERIFYPEER, False)
                    c.setopt(c.USERPWD,auth)
                    c.setopt(c.WRITEFUNCTION,f.write)
                    c.perform()
                    c.close()
            else:
                if verbose:
                    print "skipping existing file %s" % filename


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
            kmlfile = open(name+'.kml','w')
            kmlfile.write(buff)
            kmlfile.close()

        simple = shapely.wkt.loads(footprint)
        footprint_r1 = shapely.wkt.dumps(simple,rounding_precision=1)
        centroid_r1 = shapely.wkt.dumps(simple.centroid,rounding_precision=1)
        cur.execute('''INSERT OR REPLACE INTO products 
                (id,hash,name,idate,bdate,edate,ptype,direction,orbitno,relorbitno,footprint,platform,footprint_r1,centroid_r1) 
                VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?)''', 
                (uniqid,name,idate,bdate,edate,ptype,direction,orbitno,relorbitno,footprint,platform,footprint_r1,centroid_r1))
        db.commit()
    else:
        if verbose:
            print "skipping %s" % name

if list_products:
    pf.close()

db.close()
sys.exit(0)

