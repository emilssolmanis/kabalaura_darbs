SET default_parallel 10;

REGISTER ${localRepository}/com/esmupliks/${project.artifactId}/${project.version}/${project.artifactId}-${project.version}-jar-with-dependencies.jar;

DEFINE Segment com.esmupliks.pig.eval.SegmentTrack();

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
        Segment(sorted, 5.0, 3.0, 40.0, 150.0, 250.0, 3);
}

STORE gps_points_segmentation INTO '$OUT_DIR' USING PigStorage('|');
