package com.esmupliks.pig.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.esmupliks.pig.eval.SegmentTrack.Certainty;
import com.esmupliks.pig.eval.SegmentTrack.DistanceCalculator;
import com.esmupliks.pig.eval.SegmentTrack.GPSPoint;
import com.esmupliks.pig.eval.SegmentTrack.TransportationMode;
import com.javadocmd.simplelatlng.LatLng;

public class SegmentTrackTest {
    private static SegmentTrack segmentTrack;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();

    @BeforeClass
    public static void setUp() {
        segmentTrack = new SegmentTrack();
        // implement the distance calculations as Euclidean distance, just to make stuff easier
        segmentTrack.setDistanceCalculator(new DistanceCalculator() {
                public double distance(GPSPoint p1, GPSPoint p2) {
                    double x1 = p1.getPosition().getLatitude();
                    double x2 = p2.getPosition().getLatitude();
                    double y1 = p1.getPosition().getLongitude();
                    double y2 = p2.getPosition().getLongitude();
                    return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
                }
            });
    }

    @Test
    public void testBagToPoints() throws ExecException {
        DataBag pointsBag = bagFactory.newDefaultBag();

        Tuple t = tupleFactory.newTuple();
        t.append(1L);
        t.append(1.0);
        t.append(1.0);
        t.append(new DateTime());
        pointsBag.add(t);

        t = tupleFactory.newTuple();
        t.append(2L);
        t.append(2.0);
        t.append(2.0);
        t.append(new DateTime());
        pointsBag.add(t);

        t = tupleFactory.newTuple();
        t.append(3L);
        t.append(3.0);
        t.append(3.0);
        t.append(new DateTime());
        pointsBag.add(t);

        List<GPSPoint> points = segmentTrack.bagToPoints(pointsBag);
        
        for (int i = 1; i < 4; i++) {
            GPSPoint point = points.get(i - 1);
            Assert.assertEquals((long)i, point.getId());
            Assert.assertEquals((double)i, point.getPosition().getLatitude(), 0.01);
            Assert.assertEquals((double)i, point.getPosition().getLongitude(), 0.01);
        }
    }

    private List<GPSPoint> getPoints() {
        List<GPSPoint> points = new ArrayList<GPSPoint>();
        points.add(new GPSPoint(1l, 1.0, 1.0, new DateTime(2013, 1, 1, 0, 0, 0)));
        points.add(new GPSPoint(2l, 2.0, 2.0, new DateTime(2013, 1, 1, 0, 0, 1)));
        points.add(new GPSPoint(3l, 3.0, 3.0, new DateTime(2013, 1, 1, 0, 0, 3)));
        points.add(new GPSPoint(4l, 4.0, 4.0, new DateTime(2013, 1, 1, 0, 0, 6)));
        return points;
    }

    private List<Double> getSpeeds() {
        return segmentTrack.getSpeeds(getPoints());
    }

    private List<Double> getAccelerations() {
        return segmentTrack.getAccelerations(getSpeeds());
    }

    @Test
    public void testGetSpeeds() {
        List<Double> speeds = segmentTrack.getSpeeds(getPoints());
        double sqrt2 = Math.sqrt(2.0);
        Assert.assertEquals(sqrt2, speeds.get(0), 0.01);
        Assert.assertEquals(sqrt2 / 2.0, speeds.get(1), 0.01);
        Assert.assertEquals(sqrt2 / 3.0, speeds.get(2), 0.01);
    }

    @Test
    public void testGetAccelerations() {
        List<Double> speeds = getSpeeds();
        List<Double> accelerations = segmentTrack.getAccelerations(speeds);
        Assert.assertEquals(speeds.get(1) - speeds.get(0), accelerations.get(0), 0.01);
        Assert.assertEquals(speeds.get(2) - speeds.get(1), accelerations.get(1), 0.01);
    }

    @Test
    public void testTransportationModes() {
        List<Double> speeds = new ArrayList<Double>();
        speeds.add(10.0);
        speeds.add(3.0);
        List<Double> accelerations = new ArrayList<Double>();
        accelerations.add(7.0);
        accelerations.add(3.0);

        List<TransportationMode> modes = segmentTrack.getTransportationModes(speeds, accelerations, 8.0, 4.0);
        Assert.assertEquals(TransportationMode.NONWALK, modes.get(0));
        Assert.assertEquals(TransportationMode.WALK, modes.get(1));
    }

    @Test
    public void testGetBreakPoints() {
        List<TransportationMode> modes = new ArrayList<TransportationMode>();
        // construct WWW-NNNN-WW-NNN
        // bps should be { 0, 3, 7, 9, 12 }
        for (int i = 0; i < 3; i++) {
            modes.add(TransportationMode.WALK);
        }
        for (int i = 0; i < 4; i++) {
            modes.add(TransportationMode.NONWALK);
        }
        for (int i = 0; i < 2; i++) {
            modes.add(TransportationMode.WALK);
        }
        for (int i = 0; i < 3; i++) {
            modes.add(TransportationMode.NONWALK);
        }
        List<Integer> bps = segmentTrack.getBreakPoints(modes);
        int[] expecteds = { 0, 3, 7, 9, 12 };
        int[] actuals = new int[bps.size()];
        for (int i = 0; i < actuals.length; i++) {
            actuals[i] = bps.get(i);
        }
        Assert.assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void testInsignificantSegments() {
        // construct WWW-NNNN-WW-NNN
        // bps should be { 0, 3, 7, 9, 12 }
        List<TransportationMode> modes = new ArrayList<TransportationMode>();
        for (int i = 0; i < 3; i++) {
            modes.add(TransportationMode.WALK);
        }
        for (int i = 0; i < 4; i++) {
            modes.add(TransportationMode.NONWALK);
        }
        for (int i = 0; i < 2; i++) {
            modes.add(TransportationMode.WALK);
        }
        for (int i = 0; i < 3; i++) {
            modes.add(TransportationMode.NONWALK);
        }
        List<Integer> breakPoints = segmentTrack.getBreakPoints(modes);

        // construct the points so that WWW-NNNN-WW-NNN
        // the 2nd W segment gets merged into NN
        List<GPSPoint> points = new ArrayList<GPSPoint>();
        for (int i = 0; i < 12; i++) {
            points.add(new GPSPoint(i + 1, i + 1, i + 1, new DateTime(2013, 1, 1, 0, 0, i + 1)));
        }
        points.get(8).setPosition(new LatLng(8.01, 8.01));

        List<TransportationMode> newModes = 
            segmentTrack.mergeInsignificantSegments(points, modes, breakPoints, 1, 1);
        Assert.assertEquals(modes.size(), newModes.size());
        TransportationMode[] expecteds = {
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK
        };
        TransportationMode[] actuals = new TransportationMode[12];
        for (int i = 0; i < actuals.length; i++) {
            actuals[i] = newModes.get(i);
        }
        Assert.assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void testCalculateCertainties() {
        // construct WWW-NNNN-WW-NNN
        // bps should be { 0, 3, 7, 9, 12 }
        // and distances sqrt(2) * (2, 3, 1, 2)
        List<TransportationMode> modes = new ArrayList<TransportationMode>();
        for (int i = 0; i < 3; i++) {
            modes.add(TransportationMode.WALK);
        }
        for (int i = 0; i < 4; i++) {
            modes.add(TransportationMode.NONWALK);
        }
        for (int i = 0; i < 2; i++) {
            modes.add(TransportationMode.WALK);
        }
        for (int i = 0; i < 3; i++) {
            modes.add(TransportationMode.NONWALK);
        }
        List<Integer> breakPoints = segmentTrack.getBreakPoints(modes);

        List<GPSPoint> points = new ArrayList<GPSPoint>();
        for (int i = 0; i < 12; i++) {
            points.add(new GPSPoint(i + 1, i + 1, i + 1, new DateTime(2013, 1, 1, 0, 0, i + 1)));
        }

        double sqrt2 = Math.sqrt(2);
        List<Certainty> certainties = segmentTrack.calculateSegmentCertainties(points, breakPoints, 2 * sqrt2 - 0.01);
        Certainty[] expecteds = { Certainty.CERTAIN, Certainty.CERTAIN, Certainty.UNCERTAIN, Certainty.CERTAIN };
        Certainty[] actuals = new Certainty[certainties.size()];
        Assert.assertEquals(expecteds.length, actuals.length);

        for (int i = 0; i < actuals.length; i++) {
            actuals[i] = certainties.get(i);
        }

        Assert.assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void testMergeUncertain() {
        // construct WWW-NNNN-WW-NNN
        TransportationMode[] modesArr = {
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.NONWALK,
            TransportationMode.WALK,
            TransportationMode.NONWALK,
            TransportationMode.WALK
        };
        Certainty[] certaintiesArr = {
            Certainty.UNCERTAIN,
            Certainty.UNCERTAIN,
            Certainty.CERTAIN,
            Certainty.UNCERTAIN,
            Certainty.UNCERTAIN,
            Certainty.CERTAIN,
            Certainty.UNCERTAIN,
            Certainty.UNCERTAIN,
            Certainty.UNCERTAIN
        };
        List<TransportationMode> modes = Arrays.asList(modesArr);
        List<Integer> bps = segmentTrack.getBreakPoints(modes);
        List<Certainty> certainties = Arrays.asList(certaintiesArr);
        
        int numMerged = segmentTrack.mergeUncertainSegments(modes, certainties, bps, 2);
        Assert.assertEquals(3, numMerged);
        TransportationMode[] actuals = new TransportationMode[modes.size()];
        Assert.assertEquals(modesArr.length, actuals.length);
        for (int i = 0; i < actuals.length; i++) {
            actuals[i] = modes.get(i);
        }

        TransportationMode[] expecteds = {
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.WALK,
            TransportationMode.WALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK,
            TransportationMode.NONWALK
        };
    }
}
