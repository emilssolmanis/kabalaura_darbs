SET default_parallel 10;

REGISTER ${localRepository}/com/esmupliks/${project.artifactId}/${project.version}/${project.artifactId}-${project.version}-jar-with-dependencies.jar;

DEFINE Segment com.esmupliks.pig.eval.SegmentTrack();
DEFINE Haversine datafu.pig.geo.HaversineDistInMiles();
DEFINE Enumerate datafu.pig.bags.Enumerate();
DEFINE VAR datafu.pig.stats.VAR();
DEFINE Quantiles datafu.pig.stats.Quantile('0.0', '0.5', '1.0');

IMPORT 'gps_helpers.macro';

gps_points = load_gps_csv('$GPS_DIR');
-- num_order, segment, point, latitude, longitude, altitude, 
-- bearing, accuracy, speed, ToDate(time) AS time, filepath;
gps_points_segmentation = FOREACH gps_points GENERATE num_order, latitude, longitude, time, filepath;
gps_points_segmentation = GROUP gps_points_segmentation BY filepath;
gps_points_segmentation = FOREACH gps_points_segmentation 
    GENERATE 
        group AS filepath, 
        gps_points_segmentation.(num_order, latitude, longitude, time) AS points;
gps_points_segmentation = FOREACH gps_points_segmentation {
    sorted = ORDER points BY num_order ASC;
    GENERATE 
        filepath, 
        -- speedThr, accelThr, segmTimeThr, segmDistThr, segmCertaintyDist, uncertainMergeThr
        -- double,   double,   double,      double,      double,            int
        FLATTEN(Enumerate(Segment(sorted, $SPEED_THR, $ACCEL_THR, $SEG_TIME_THR, $SEG_DIST_THR, $SEG_CERT_DIST, $UNCERT_MERGE_THR))) AS (transport_mode, certainty, points, segment_id);
}

num_segments = FOREACH gps_points_segmentation GENERATE filepath, segment_id;
num_segments = DISTINCT num_segments;
num_segments = GROUP num_segments BY filepath;
num_segments = FOREACH num_segments GENERATE COUNT(num_segments) AS num;
num_segments = GROUP num_segments ALL;
num_segments = FOREACH num_segments {
    sorted = ORDER num_segments BY num;
    GENERATE AVG(sorted), SQRT(VAR(sorted)), Quantiles(sorted);
}
STORE num_segments INTO '$OUT_DIR/evaluation';

-- DESCRIBE gps_points_segmentation;
STORE gps_points_segmentation INTO '$OUT_DIR/segments' USING PigStorage('|');

gps_points = FOREACH gps_points_segmentation GENERATE filepath, segment_id, transport_mode, certainty, FLATTEN(points) AS (point_id, lat, lon, time);
-- DESCRIBE gps_points;

prev_points = FOREACH gps_points GENERATE filepath, segment_id, transport_mode, certainty, point_id + 1 AS point_id, lat, lon, time;

joined_points = JOIN gps_points BY (filepath, segment_id, point_id), prev_points BY (filepath, segment_id, point_id);
joined_points = FOREACH joined_points 
    GENERATE 
        gps_points::filepath AS filepath,
        gps_points::segment_id AS segment_id,
        gps_points::transport_mode AS transport_mode,
        gps_points::certainty AS certainty,
        gps_points::point_id AS point_id,
        gps_points::lat AS lat_curr, 
        gps_points::lon AS lon_curr,
        gps_points::time AS time_curr,
        prev_points::lat AS lat_prev,
        prev_points::lon AS lon_prev,
        prev_points::time AS time_prev;

speeds = FOREACH joined_points
    GENERATE
        filepath, segment_id, transport_mode, certainty, point_id,
        Haversine(lat_curr, lon_curr, lat_prev, lon_prev) * 1609.344 AS dist_meters,
        MilliSecondsBetween(time_curr, time_prev) AS time_between;
speeds = FILTER speeds BY time_between > 0;
speeds = FOREACH speeds
    GENERATE
        filepath, segment_id, transport_mode, certainty, point_id,
        dist_meters / ((float)time_between / 1000) AS speed;

-- DESCRIBE speeds;
speeds = ORDER speeds BY filepath ASC, segment_id ASC, point_id ASC;
STORE speeds INTO '$OUT_DIR/speeds' USING PigStorage('|');

prev_speeds = FOREACH speeds GENERATE filepath, segment_id, transport_mode, certainty, point_id + 1 AS point_id, speed;

joined_speeds = JOIN speeds BY (filepath, segment_id, point_id), prev_speeds BY (filepath, segment_id, point_id);
accelerations = FOREACH joined_speeds
    GENERATE
        speeds::filepath AS filepath,
        speeds::segment_id AS segment_id,
        speeds::transport_mode AS transport_mode,
        speeds::certainty AS certainty,
        speeds::point_id AS point_id,
        speeds::speed - prev_speeds::speed AS acceleration;

-- DESCRIBE accelerations;
accelerations = ORDER accelerations BY filepath ASC, segment_id ASC, point_id ASC;
STORE accelerations INTO '$OUT_DIR/accelerations' USING PigStorage('|');
