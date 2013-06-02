#!/usr/bin/env python2
import sys

def parse_line(line):
    filepath, tr_mode, certainty, points, segment_id = line.split('|')
    points = points.split('),(')
    if not points:
        raise StopIteration
    points = sorted(((int(p[0]), float(p[1]), float(p[2])) for p in (p.split(',') for p in points[2:-3])), key=lambda p: p[0])
    for pid, lat, lon in points:
        yield (filepath, int(segment_id), tr_mode, pid, lat, lon)

# (transport_mode, certainty, points, segment_id)
def parse(iterable):
    for line in iterable:
        for point in parse_line(line):
            yield point

def main():
    for point in parse(sys.stdin):
        print '%s,%d,%s,%d,%.7f,%.7f' % point


if __name__ == '__main__':
    main()
