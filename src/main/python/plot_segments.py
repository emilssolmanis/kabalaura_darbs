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
m = Basemap(projection='merc', llcrnrlat=56.85, urcrnrlat=57,\
            llcrnrlon=23.95, urcrnrlon=24.2, lat_ts=20, resolution=None)
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
    filename, tr_mode, certainty, points, segment_id = line.split('|')
    points = [gps_tuple.split(',') for gps_tuple in points[2:-2].split('),(')]
    points = [(float(p[2]), float(p[1])) for p in points]
    if len(points) > 2:
        xx, yy = zip(*points)
        # red for non-walk
        color = 'r' if tr_mode == '1' else 'g'
        # thick for certain
        linewidth = 1 if certainty == '0' else 0.5
        label = '$%2d$' % int(segment_id)
        m.scatter(xx[0], yy[0], color='black', marker=label, latlon=True, s=150)
        m.plot(xx, yy, linewidth=linewidth, color=color, alpha=0.5, latlon=True)


# for line in sys.stdin:
#     filename = line.split('|')[0]
#     print(filename)
#     data = line.split('|')[1]
#     segments = data.split(')}),(')
#     segments[0] = segments[0][2:]
#     segments[-1] = segments[-1][:-4]
#     for segment in segments:
#         non_walk = bool(int(segment[0]))
#         uncertain = bool(int(segment[2]))
#         segment = segment[6:]
#         segment = segment.split('),(')
#         segment = [p.split(',') for p in segment]
#         segment = [(int(p[0]), float(p[1]), float(p[2])) for p in segment]
#         segment.sort(key=lambda elem: elem[0])
#         xx = [p[2] for p in segment]
#         yy = [p[1] for p in segment]
#         xx, yy = m(xx, yy)
#         color = 'r' if non_walk else 'g'
#         linewidth = 1 if not uncertain else 0.5
#         m.plot(xx, yy, linewidth=linewidth, color=color)

plt.show()
