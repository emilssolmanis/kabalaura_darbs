#!/usr/bin/env python2
from __future__ import print_function

import sys

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap

# llcrnrlat,llcrnrlon,urcrnrlat,urcrnrlon
# are the lat/lon values of the lower left and upper right corners
# of the map.
# lat_ts is the latitude of true scale.
# resolution = 'c' means use crude resolution coastlines.
m = Basemap(projection='merc', llcrnrlat=55.6, urcrnrlat=58,\
            llcrnrlon=21, urcrnrlon=28, lat_ts=20, resolution=None)
# create figure.
fig = plt.figure()
# read shapefile.
# shp_info = m.readshapefile(sys.argv[1], 'roads', drawbounds=False)

# names = []
# for road in m.roads_info:
#     print(road)

# for road in m.roads[:10]:
#     xx, yy = zip(*road)
#     m.plot(xx, yy, linewidth=0.5, color='black')

for line in sys.stdin:
    data = line.split('|')[1]
    segments = data.split(')}),(')
    segments[0] = segments[0][2:]
    segments[-1] = segments[-1][:-4]
    for segment in segments:
        non_walk = bool(int(segment[0]))
        segment = segment[4:]
        segment = segment.split('),(')
        segment = [p.split(',') for p in segment]
        segment = [(int(p[0]), float(p[1]), float(p[2])) for p in segment]
        segment.sort(key=lambda elem: elem[0])
        xx = [p[2] for p in segment]
        yy = [p[1] for p in segment]
        xx, yy = m(xx, yy)
        color = 'r' if non_walk else 'g'
        m.plot(xx, yy, linewidth=1, color=color)

plt.show()
