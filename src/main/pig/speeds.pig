SET default_parallel 10;

REGISTER ${localRepository}/com/esmupliks/${project.artifactId}/${project.version}/${project.artifactId}-${project.version}-jar-with-dependencies.jar;

DEFINE Haversine datafu.pig.geo.HaversineDistInMiles();

IMPORT 'gps_helpers.macro';

gps_points = load_gps_csv('$GPS_DIR');
gps_dummy1 = FOREACH gps_points 
              GENERATE
                  num_order, filepath, latitude, longitude, time;

gps_dummy2 = FOREACH gps_points 
             GENERATE
                 num_order + 1 AS num_order, filepath, latitude, longitude, time;

gps_points_next = JOIN gps_dummy1 BY (filepath, num_order), gps_dummy2 BY (filepath, num_order);
gps_points_next = FOREACH gps_points_next
                  GENERATE
                      gps_dummy1::num_order AS num1,
                      gps_dummy1::filepath AS filepath,
                      gps_dummy1::latitude AS lat1,
                      gps_dummy1::longitude AS lon1,
                      gps_dummy2::latitude AS lat2,
                      gps_dummy2::longitude AS lon2,
                      gps_dummy1::time AS time1,
                      gps_dummy2::time AS time2;
gps_points_next = FOREACH gps_points_next
                  GENERATE
                      num1 AS num, filepath, lat1, lon1, lat2, lon2, time1, time2,
                      Haversine(lat1, lon1, lat2, lon2) * 1609.344 AS dist,
                      MilliSecondsBetween(time1, time2) AS time_between;
gps_points_next = FILTER gps_points_next BY time_between > 0.0;
gps_points_next = FOREACH gps_points_next
                  GENERATE
                      num, filepath, lat1, lon1, lat2, lon2, time1, time2, dist, time_between,
                      dist / ((float)time_between / 1000) AS speed;

STORE gps_points_next INTO '$OUT_DIR' USING PigStorage('|');
