#!/usr/bin/env python
#
#   Copyright (C) 2016 Francesco P. Lovergine <f.lovergine@ba.issia.cnr.it>
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
import os.path
import sqlite3 as sqlite
import osgeo.ogr as ogr
import osgeo.osr as osr
import shapely.wkt
import re
import time
import math

#
#   This easy program outputs a proper stacking of S-1 images for
#   interferometric purposes (or any other multi-temporal analysis).
#   It simply uses a single direction and image type and requires a
#   parametric minimum area for intersecting frames. S-1 fingerprints
#   are taken from the scihub database, populated at download time.
#   So it is fast enough for stacking and selecting images on fly.
#

def usage():
    print '''usage: %s [-m master|-A area|-h|-d database|-a|-v|-t {GRD|SLC}|-W]
[--master=master|--area=area|--help|--database=database|--warranty
 --auto|--verbose|--type=GRD|SLC]
''' % sys.argv[0]

def help():
    print '''
This is free software; see the source code for copying conditions.
There is ABSOLUTELY NO WARRANTY; not even for MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  For details, use --warranty.

usage: %s [-m master|-A area|-h|-d database|-a|-v|-t GRD|SLC|-W]
[--master=master|--area=area|--help|--database=database|--auto|--verbose|--warranty]

Options are:

    -m --master=<master_image> [nodefault]
    -d --database=<scihub_db> [./scihub.sqlite]
    -h --help 
    -A --area=<value_kmq> [40mil kmq]
    -a --auto 
    -v --verbose
    -W --warranty
    -t --type=<SLC|GRD> [GRD]
''' % sys.argv[0]

def warranty():
    print '''
Copyright (C) 2016 Francesco Paolo Lovergine and others.

This is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

'''
products = []

# this is a good compromise for defining a proper stacking, in kmq
area = 40000
kmq = 1000000

master = None
direction = None
verbose = False
db_file = 'scihub.sqlite'
auto = False
ptype = 'GRD'

try:
    opts, args = getopt.getopt(sys.argv[1:],'m:A:hd:avt:W',
            ['master','area','help','database','auto','verbose','type',
            'warranty'])
except getopt.GetoptError:
    help()
    sys.exit(3)

for opt, arg in opts:
    if opt in ['-m','--master']:
        master = arg
    if opt in ['-A','--area']:
        area = float(arg)
    if opt in ['-a','--auto']:
        auto = True
    if opt in ['-d','--database']:
        db_file = arg
    if opt in ['-v','--verbose']:
        verbose = True
    if opt in ['-t', '--type']:
        ptype = arg.upper()
        if not ptype in ['GRD','SLC']:
            usage()
            sys.exit(4)
    if opt in ['-h','--help']:
        help()
        sys.exit(5)
    if opt in ['-W','--warranty']:
        warranty()
        sys.exit(2)

if not auto and master == None:
    help()
    sys.exit(6)

try:
    db = sqlite.connect(db_file)
except sqlite.Error, e:
    print 'Error %s:' % e.args[0]
    sys.exit(1)

if not auto:

    cur = db.cursor()
    cur.execute('''SELECT name,footprint,relorbitno,direction,ptype 
                    FROM products WHERE platform = 'Sentinel-1' 
                    AND name = '%s' ORDER BY idate ASC LIMIT 1''' % (master))
    m = cur.fetchone()
    if m == None:
        print "Master not found"
        sys.exit(7)

    if verbose:
        print m[0], m[1], 'MASTER', m[2], m[3], m[4]
    else:
        print m[0]

    direction = m[3]
    ptype = m[4]

    src = osr.SpatialReference()
    src.ImportFromEPSG(4326)

    dst = osr.SpatialReference()
    dst.ImportFromEPSG(3410)

    trans = osr.CoordinateTransformation(src,dst)
    invtrans = osr.CoordinateTransformation(dst,src)

    mpoly = ogr.CreateGeometryFromWkt(m[1])
    mpoly.Transform(trans)

    for rec in cur.execute('''SELECT name,footprint,relorbitno,direction,ptype 
            FROM products WHERE platform = 'Sentinel-1' AND
            direction = '%s' AND name <> '%s' and ptype = '%s' 
            ORDER BY bdate ASC''' % (direction, master, ptype)):

        poly = ogr.CreateGeometryFromWkt(rec[1])
        poly.Transform(trans)

        inters = mpoly.Intersection(poly)

        if inters.GetArea()/kmq >= area: # kmq
            if verbose:
                inters.Transform(invtrans)
                print rec[0], rec[1], inters.ExportToWkt(), rec[2], rec[3], \
                      rec[4], area
            else:
                print rec[0]
    
else:
    
    acur = db.cursor()
    ascs = acur.execute('''SELECT name,footprint,relorbitno,direction,ptype 
            FROM products WHERE platform = 'Sentinel-1' AND
            direction = '%s' and ptype = '%s' ORDER BY bdate ASC''' %
            ('ASCENDING',ptype))
    bcur = db.cursor()
    descs = bcur.execute('''SELECT name,footprint,relorbitno,direction,ptype 
            FROM products WHERE platform = 'Sentinel-1' AND
            direction = '%s' and ptype = '%s' ORDER BY bdate ASC''' %
            ('DESCENDING',ptype))

    src = osr.SpatialReference()
    src.ImportFromEPSG(4326)

    dst = osr.SpatialReference()
    dst.ImportFromEPSG(3410)

    trans = osr.CoordinateTransformation(src,dst)
    invtrans = osr.CoordinateTransformation(dst,src)

    acluster = {}
    dcluster = {}
    aframes = set()
    dframes = set()
    d_asc = {}
    d_desc = {}
    for asc in ascs:
        d_asc[asc[0]] = [asc[1],asc[2],asc[3],asc[4]]
        aframes.add(asc[0])
    for desc in descs:
        d_desc[desc[0]] = [desc[1],desc[2],desc[3],desc[4]]
        dframes.add(desc[0])

    # ascending frames 

    while aframes:
        target = aframes.pop()
        aframes2 = aframes.copy()
        if verbose:
            print target
        acluster[target] = []
        acluster[target].append(target)
        if verbose:
            print d_asc[target][0]
        mpoly = ogr.CreateGeometryFromWkt(d_asc[target][0])
        mpoly.Transform(trans)
        for val in aframes:
            if d_asc[val][2] == d_asc[target][2] and \
            d_asc[val][3] == d_asc[target][3]:
                poly = ogr.CreateGeometryFromWkt(d_asc[val][0])
                poly.Transform(trans)
                inters = mpoly.Intersection(poly)
                a = inters.GetArea()/kmq
                if a >= area: 
                    acluster[target].append(val)
                    aframes2.remove(val)
                    if verbose:
                        print 'added %s to %s ASC stack with area %.2f' % (val,target,a)
        aframes = aframes2.copy()

    # descending frames

    while dframes:
        target = dframes.pop()
        dframes2 = dframes.copy()
        if verbose:
            print target
        dcluster[target] = []
        dcluster[target].append(target)
        if verbose:
            print d_desc[target][0]
        mpoly = ogr.CreateGeometryFromWkt(d_desc[target][0])
        mpoly.Transform(trans)
        for val in dframes:
            if d_desc[val][2] == d_desc[target][2] and \
               d_desc[val][3] == d_desc[target][3]:
                poly = ogr.CreateGeometryFromWkt(d_desc[val][0])
                poly.Transform(trans)
                inters = mpoly.Intersection(poly)
                a = inters.GetArea()/kmq
                if a >= area: 
                    dcluster[target].append(val)
                    dframes2.remove(val)
                    if verbose:
                        print 'added %s to %s DESC stack with area %.2f' % (val,target,a)
        dframes = dframes2.copy()

    # output all clusters, with time ordering and using the oldest as master

    for clust in acluster:
        acluster[clust].sort()
    for clust in dcluster:
        dcluster[clust].sort()
    for clust in acluster:
        print acluster[clust][0] + '\t' + '(ASC)'
        for frame in acluster[clust]:
            print '\t',frame
    for clust in dcluster:
        print dcluster[clust][0] + '\t' + '(DESC)'
        for frame in dcluster[clust]:
            print '\t',frame

db.close()
sys.exit(0)

