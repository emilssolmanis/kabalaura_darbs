#!/usr/bin/env python2
import sys

def parse_line(line):
    filename, segment_id, num_points, points_str = line.split('|')
    points_str = points_str[2:-3]
    powers = [float(p.split(',')[2]) for p in points_str.split('),(')]
    return '%s,%s,%s,%s' % (filename, segment_id, num_points, ','.join(str(power) for power in powers))


def parse(iterable):
    for line in iterable:
        yield parse_line(line)


def main():
    for record in parse(sys.stdin):
        print record

if __name__ == '__main__':
    main()
