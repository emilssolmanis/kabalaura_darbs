#!/usr/bin/env python2
from __future__ import print_function

import sys

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap

def main():
    cluster_colors = {1: 'black', 2: 'red', 3: 'blue', 4: 'green', 5: 'purple', 6: 'orange'}
    clusters = dict()
    with open(sys.argv[1]) as cf:
        for line in cf:
            filename, segment_id, cluster_id = line.strip().split(',')
            segment_id = int(segment_id)
            cluster_id = int(cluster_id)
            clusters[(filename, segment_id)] = cluster_id

    # llcrnrlat,llcrnrlon,urcrnrlat,urcrnrlon
    # are the lat/lon values of the lower left and upper right corners
    # of the map.
    # lat_ts is the latitude of true scale.
    # resolution = 'c' means use crude resolution coastlines.
    m = Basemap(projection='merc', llcrnrlat=56.85, urcrnrlat=57,\
                llcrnrlon=23.95, urcrnrlon=24.2, lat_ts=20, resolution=None)
    # create figure.
    fig = plt.figure()

    for line in sys.stdin:
        filename, tr_mode, certainty, points, segment_id = line.split('|')
        segment_id = int(segment_id)
        points = [gps_tuple.split(',') for gps_tuple in points[2:-2].split('),(')]
        points = [(float(p[2]), float(p[1])) for p in points]
        if len(points) > 2 and (filename, segment_id) in clusters:
            cluster_id = clusters[(filename, segment_id)]
            color = cluster_colors[cluster_id]
            xx, yy = zip(*points)
            # thick for certain
            linewidth = 1 if certainty == '0' else 0.5
            # label = '$%2d$' % int(segment_id)
            # m.scatter(xx[0], yy[0], color='black', marker=label, latlon=True, s=150)
            m.plot(xx, yy, linewidth=linewidth, color=color, alpha=0.2, latlon=True)
    plt.show()


if __name__ == '__main__':
    main()
