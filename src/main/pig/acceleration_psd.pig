-- * <ul>
-- * <li>a sorted bag of tuples to take the PSD of, assumed to have less than 2^31 elements</li>
-- * <li>the field of the bag's tuples to take the PSD of</li>
-- * <li>the cosine-bell taper percentage</li>
-- * <li>1 or more modified Daniell filter sizes (odd integers) to apply</li>
-- * </ul>
SET default_parallel 10;

REGISTER ${localRepository}/com/esmupliks/${project.artifactId}/${project.version}/${project.artifactId}-${project.version}-jar-with-dependencies.jar;

DEFINE PSD com.esmupliks.pig.eval.PowerSpectralDensityPeriodogram();

accels = LOAD '$ACCEL_DIR' USING PigStorage('|') AS (filepath:chararray, segment_id:long, transport_mode:int, certainty:int, point_id:long, acceleration:double);

accels = GROUP accels BY (filepath, segment_id);
accels = FOREACH accels
    GENERATE 
        group.filepath AS filepath, 
        group.segment_id AS segment_id,
        accels.(point_id, acceleration) AS data_points;
psds = FOREACH accels {
    sorted = ORDER data_points BY point_id ASC;
    GENERATE filepath, segment_id, FLATTEN(PSD(sorted, 1, 0.1, 5, 9)) AS (idx, frequency, power);
}

STORE psds INTO '$OUT_DIR' USING PigStorage('|');
