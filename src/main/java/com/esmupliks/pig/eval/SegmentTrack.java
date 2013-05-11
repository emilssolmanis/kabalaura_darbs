package com.esmupliks.pig.eval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

import com.esmupliks.pig.eval.SegmentTrack.Certainty;
import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;

public class SegmentTrack extends EvalFunc<DataBag> {
    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private interface UniqueID {
        public long getId();
    }

    public static enum TransportationMode implements UniqueID {
        WALK {
            public long getId() { return 0; }
        },
        NONWALK {
            public long getId() { return 1; }
        }
    }

    public static enum Certainty implements UniqueID {
        CERTAIN { public long getId() { return 0; } },
        UNCERTAIN { public long getId() { return 1; } };
    }

    public static final class GPSPoint {
        public GPSPoint(long id, double lat, double lon, DateTime time) {
            this.id = id;
            this.position = new LatLng(lat, lon);
            this.time = time;
        }

        private LatLng position;
        private DateTime time;
        private long id;
        
        public long getId() {
            return this.id;
        }
        
        public void setId(long id) {
            this.id = id;
        }
        
        public LatLng getPosition() {
            return position;
        }
        
        public void setPosition(LatLng position) {
            this.position = position;
        }
        
        public DateTime getTime() {
            return time;
        }
        
        public void setTime(DateTime time) {
            this.time = time;
        }
        
        public String toString() {
            return String.format("%d - %s - %s", id, position, time);
        }
    }

    public static interface DistanceCalculator {
        public double distance(GPSPoint p1, GPSPoint p2);
    }

    private static final class DefaultDistanceCalculator implements DistanceCalculator {
        public double distance(GPSPoint p1, GPSPoint p2) {
            return LatLngTool.distance(p1.getPosition(), p2.getPosition(), LengthUnit.METER);
        }
    }

    private DistanceCalculator distanceCalculator;

    public SegmentTrack() {
        this.distanceCalculator = new DefaultDistanceCalculator();
    }

    public void setDistanceCalculator(DistanceCalculator distanceCalculator) {
        this.distanceCalculator = distanceCalculator;
    }

    public List<Double> getSpeeds(List<GPSPoint> points) {
        List<Double> speeds = new ArrayList<Double>();

        GPSPoint prev = points.get(0);
        for (GPSPoint curr : points.subList(1, points.size())) {
            double distance = distanceCalculator.distance(curr, prev);
            int timeBetween = Seconds.secondsBetween(prev.getTime(), curr.getTime()).getSeconds();
            speeds.add(distance / timeBetween);
            prev = curr;
        }

        return speeds;
    }

    public List<Double> getAccelerations(List<Double> speeds) {
        List<Double> accelerations = new ArrayList<Double>();

        double prev = speeds.get(0);
        for (double curr : speeds.subList(1, speeds.size())) {
            accelerations.add(curr - prev);
            prev = curr;
        }

        return accelerations;
    }

    public List<GPSPoint> bagToPoints(DataBag bag) throws ExecException {
        List<GPSPoint> points = new ArrayList<GPSPoint>();

        for(Tuple t : bag) {
            Long id = (Long) t.get(0);
            Double lat = (Double) t.get(1);
            Double lon = (Double) t.get(2);
            DateTime time = (DateTime) t.get(3);
            points.add(new GPSPoint(id, lat, lon, time));
        }

        Collections.sort(points, new Comparator<GPSPoint>() {
                public int compare(GPSPoint lhs, GPSPoint rhs) {
                    return lhs.time.compareTo(rhs.time);
                }
            });

        return points;
    }

    private double distanceSum(List<GPSPoint> points) {
        double sum = 0;
        for (int i = 0; i < points.size() - 1; i++) {
            GPSPoint curr = points.get(i);
            GPSPoint next = points.get(i + 1);
            sum += distanceCalculator.distance(curr, next);
        }
        return sum;
    }

    public List<TransportationMode> mergeInsignificantSegments(List<GPSPoint> points,
                                                                List<TransportationMode> modes,
                                                                List<Integer> breakPoints,
                                                                double distanceThreshold,
                                                                double timeThreshold) {
        List<TransportationMode> mergedModes = new ArrayList<>(modes.size());
        mergedModes.addAll(modes);

        // there's nothing to merge the 0-th segment back onto, start from 1st
        for (int i = 1; i < breakPoints.size() - 1; i++) {
            int start = breakPoints.get(i);
            int end = breakPoints.get(i + 1);
            GPSPoint startPoint = points.get(start);
            GPSPoint endPoint = points.get(end - 1);
            double distance = distanceSum(points.subList(start, end));
            int time = Seconds.secondsBetween(startPoint.getTime(), endPoint.getTime()).getSeconds();
            if (time < timeThreshold || distance < distanceThreshold) {
                int prevStart = breakPoints.get(i - 1);
                TransportationMode newMode = modes.get(prevStart);
                for (int j = start; j < end; j++) {
                    mergedModes.set(j, newMode);
                }
            }
        }

        return mergedModes;
    }

    public List<Integer> getBreakPoints(List<TransportationMode> modes) {
        List<Integer> breaks = new ArrayList<Integer>();

        if (modes.isEmpty()) {
            breaks.add(0);
            return breaks;
        }

        TransportationMode prev = null;
        for (int i = 0; i < modes.size(); i++) {
            TransportationMode curr = modes.get(i);
            if (prev != curr) {
                breaks.add(i);
            }
            prev = curr;
        }
        breaks.add(modes.size());

        return breaks;
    }

    public List<TransportationMode> getTransportationModes(List<Double> speeds, 
                                                            List<Double> accelerations,
                                                            double speedThreshold,
                                                            double accelerationThreshold) {
        List<TransportationMode> modes = new ArrayList<TransportationMode>();

        for (int i = 0; i < accelerations.size(); i++) {
            double acceleration = Math.abs(accelerations.get(i));
            double speed = speeds.get(i);

            TransportationMode mode = TransportationMode.WALK;
            if (acceleration > accelerationThreshold || speed > speedThreshold) {
                mode = TransportationMode.NONWALK;
            }

            modes.add(mode);
        }

        return modes;
    }

    public List<Certainty> calculateSegmentCertainties(List<GPSPoint> points,
                                                        List<Integer> breakPoints,
                                                        double certaintyDistanceThreshold) {
        List<Certainty> certainties = new ArrayList<Certainty>();

        for (int i = 0; i < breakPoints.size() - 1; i++) {
            int start = breakPoints.get(i);
            int end = breakPoints.get(i + 1);
            double segmentDistance = distanceSum(points.subList(start, end));
            certainties.add(segmentDistance > certaintyDistanceThreshold ? 
                            Certainty.CERTAIN : Certainty.UNCERTAIN);
        }

        return certainties;
    }

    public int mergeUncertainSegments(List<TransportationMode> modes,
                                      List<Certainty> certainties,
                                      List<Integer> breakPoints,
                                      int uncertainMergeThreshold) {
        int numMerges = 0;
        int startSegment = 0;
        Certainty prev = certainties.get(0);
        Certainty curr = null;
        for (int i = 1; i < certainties.size(); i++) {
            curr = certainties.get(i);
            /* upon certainty change point check if previous span was
             * 1) a span of uncertain segments
             * 2) longer than the threshold
             *
             * if it was, merge them all into a non-walk segment
             */
            if (curr != prev) {
                if (prev == Certainty.UNCERTAIN && (i - startSegment) >= uncertainMergeThreshold) {
                    numMerges++;

                    int startPoint = breakPoints.get(startSegment);
                    int endPoint = breakPoints.get(i);

                    for (int j = startPoint; j < endPoint; j++) {
                        modes.set(j, TransportationMode.NONWALK);
                    }
                }

                startSegment = i;
            }
            prev = curr;
        }

        // check if the tail is not an uncertain segment above threshold
        if (curr == Certainty.UNCERTAIN && (certainties.size() - startSegment) >= uncertainMergeThreshold) {
            int startPoint = breakPoints.get(startSegment);
            for (int j = startPoint; j < modes.size(); j++) {
                modes.set(j, TransportationMode.NONWALK);
            }
            numMerges++;
        }

        return numMerges;
    }

    /** Splits a GPS log into segments based on movement modes.
     * 
     * @param input A sorted bag of {(ID, lat, lon, time)}
     * @param speedThreshold Used for initial transportation mode classification, if point has
     * velocity above this, it's considered to be non-walking, given in meters / second
     * @param accelerationThreshold Used for initial transportation mode classification, if point
     * has acceleration above this, it's considered non-walking, given in meters / second^2
     * @param segmentTimeThreshold Segments lasting less than this are merged backwards into
     * the previous segment, given in seconds
     * @param segmentDistanceThreshold Segments spanning less than this distance are merged 
     * backwards into the previous segment, given in meters
     * @param segmentCertaintyDistanceThreshold Segments spanning distance below this are considered
     * uncertain, given in meters
     * @param uncertainSegmentMergeNonWalkThreshold The merge threshold for continuous uncertain
     * segments. A continuous span of uncertain segments of size larger than this will be merged
     * into a single non-walk segment.
     * @return A bag of bags, {{(ID, lat, lon, time)}, ...} containing the segments
     */
    @Override
    public DataBag exec(Tuple input) throws ExecException {
        List<GPSPoint> points = bagToPoints((DataBag) input.get(0));
        List<Double> speeds = getSpeeds(points);
        List<Double> accelerations = getAccelerations(speeds);
        
        /* Presume that we bind velocity & acceleration to points starting from the start,
         * i.e., given p, v, a and a track
         * 
         *   x----------x---------------x------------x----------x
         *   p1         p2              p3           p4         p5
         *   v1         v2              v3           v4
         *   a1         a2              a3
         * the binding looks like this, even though technically the correct version
         * would be 
         * 
         *   x----------x---------------x------------x----------x
         *   p1         p2              p3           p4         p5
         *        v1           v2             v3          v4
         *               a1            a2           a3
         */

        double speedThreshold = (Double) input.get(1);
        double accelerationThreshold = (Double) input.get(2);

        List<TransportationMode> modes = getTransportationModes(speeds, accelerations, 
                                                                speedThreshold,  
                                                                accelerationThreshold);
        List<Integer> breakPoints = getBreakPoints(modes);

        double segmentTimeThreshold = (Double) input.get(3);
        double segmentDistanceThreshold = (Double) input.get(4);

        modes = mergeInsignificantSegments(points, modes, breakPoints, segmentDistanceThreshold, 
                                           segmentTimeThreshold);
        breakPoints = getBreakPoints(modes);

        double segmentCertaintyDistanceThreshold = (Double) input.get(5);

        List<Certainty> certainties = calculateSegmentCertainties(points, breakPoints, 
                                                                  segmentCertaintyDistanceThreshold);
        
        int uncertainSegmentMergeNonWalkThreshold = (Integer) input.get(6);

        // keep merging until all segments are certain
        // CUSTOM: multiple merge passes
        int numMerged = 1;
        while (numMerged > 0) {
            numMerged = mergeUncertainSegments(modes, certainties, breakPoints, uncertainSegmentMergeNonWalkThreshold);
            breakPoints = getBreakPoints(modes);
            certainties = calculateSegmentCertainties(points, breakPoints, 
                                                      segmentCertaintyDistanceThreshold);
        }

        DataBag segments = bagFactory.newDefaultBag();
        int start = breakPoints.get(0);

        for (int i = 1; i < breakPoints.size(); i++) {
            int end = breakPoints.get(i);

            DataBag segmentPoints = bagFactory.newDefaultBag();
            for (int j = start; j < end; j++) {
                Tuple pointTuple = tupleFactory.newTuple();
                GPSPoint point = points.get(j);
                pointTuple.append(point.getId());
                pointTuple.append(point.getPosition().getLatitude());
                pointTuple.append(point.getPosition().getLongitude());
                pointTuple.append(point.getTime());
                segmentPoints.add(pointTuple);
            }
            Tuple segmentInfo = tupleFactory.newTuple();
            segmentInfo.append(modes.get(start).getId());
            segmentInfo.append(certainties.get(i - 1).getId());
            segmentInfo.append(segmentPoints);
            segments.add(segmentInfo);

            start = end;
        }

        return segments;
    }

    @Override
    public Schema outputSchema(Schema schema) {
        try {
            Schema outputBagSchema = new Schema();
            Schema segmentTupleSchema = new Schema();
            Schema singleSegmentBagSchema = new Schema();
            Schema gpsPoingTupleSchema = new Schema();
            
            gpsPoingTupleSchema.add(new FieldSchema("id", DataType.LONG));
            gpsPoingTupleSchema.add(new FieldSchema("lat", DataType.DOUBLE));
            gpsPoingTupleSchema.add(new FieldSchema("lon", DataType.DOUBLE));
            gpsPoingTupleSchema.add(new FieldSchema("time", DataType.DATETIME));
            
            singleSegmentBagSchema.add(new FieldSchema("gps_point", gpsPoingTupleSchema, DataType.TUPLE));
            segmentTupleSchema.add(new FieldSchema("transport_mode", DataType.LONG));
            segmentTupleSchema.add(new FieldSchema("certainty", DataType.LONG));
            segmentTupleSchema.add(new FieldSchema("points", singleSegmentBagSchema, DataType.BAG));

            outputBagSchema.add(new FieldSchema("segment", segmentTupleSchema, DataType.TUPLE));
            return new Schema(new FieldSchema("segments", outputBagSchema, DataType.BAG));
        } catch (FrontendException e) {
            return null;
        }
    }
}
