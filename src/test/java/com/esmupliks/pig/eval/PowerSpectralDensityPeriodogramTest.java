package com.esmupliks.pig.eval;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

public class PowerSpectralDensityPeriodogramTest {
    private PowerSpectralDensityPeriodogram psd = new PowerSpectralDensityPeriodogram();
    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testDemean() {
        double[] d = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        double[] ex = {-2.5, -1.5, -0.5, 0.5, 1.5, 2.5};
        psd.demean(d);
        Assert.assertArrayEquals(ex, d, 0.001);
    }

    @Test
    public void testTaper() {
        double[] d = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
        double[] ex = {0.14644, 1.7071, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 7.68198, 1.46446};
        psd.taper(d, 0.2);
        Assert.assertArrayEquals(ex, d, 0.001);
    }

    @Test
    public void closestPowerOf2() {
        Assert.assertEquals(11, psd.closestPowerOf2(1025));
        Assert.assertEquals(10, psd.closestPowerOf2(1024));
        Assert.assertEquals(10, psd.closestPowerOf2(999));
        Assert.assertEquals(5, psd.closestPowerOf2(32));
        Assert.assertEquals(5, psd.closestPowerOf2(28));
        Assert.assertEquals(5, psd.closestPowerOf2(24));
        Assert.assertEquals(5, psd.closestPowerOf2(17));
        Assert.assertEquals(4, psd.closestPowerOf2(16));
        Assert.assertEquals(4, psd.closestPowerOf2(15));
        Assert.assertEquals(4, psd.closestPowerOf2(9));
        Assert.assertEquals(2, psd.closestPowerOf2(4));
    }

    @Test
    public void zeroPad() {
        double[] arr0 = {1.0, 2.0, 3.0, 4.0};
        double[] ex0 = {1.0, 2.0, 3.0, 4.0};
        double[] act = psd.zeroPadToPow2(arr0, 2);
        Assert.assertArrayEquals(ex0, act, 0.0001);

        double[] arr1 = {1.0, 2.0, 3.0, 4.0, 5.0};
        double[] ex1 = {1.0, 2.0, 3.0, 4.0, 5.0, 0.0, 0.0, 0.0};
        act = psd.zeroPadToPow2(arr1, 3);
        Assert.assertArrayEquals(ex1, act, 0.0001);
    }

    @Test
    public void testDaniell() throws ExecException {
        int[] lens0 = {3};
        double[] ex0 = {0.25, 0.5, 0.25};
        double[] d0 = psd.modifiedDaniell(lens0);
        Assert.assertArrayEquals(ex0, d0, 0.001);

        int[] lens1 = {5, 7, 9};
        double[] ex1 = {0.000651, 0.003906, 0.011719, 0.024740, 0.042318, 0.062500, 0.082682, 0.100260, 0.112630, 0.117187, 0.112630, 0.100260, 0.082682, 0.062500, 0.042318, 0.024740, 0.011719, 0.003906, 0.000651};
        double[] d1 = psd.modifiedDaniell(lens1);
        Assert.assertArrayEquals(ex1, d1, 0.001);
        
        int[] lens2 = {7, 9, 5};
        double[] d2 = psd.modifiedDaniell(lens2);
        
        Assert.assertArrayEquals(ex1, d2, 0.001);
        Assert.assertArrayEquals(d1, d2, 0.001);
    }

    @Test
    public void testConvolve() {
        double[] x = {0.125, 0.25, 0.25, 0.25, 0.125};
        double[] f = {0.25, 0.5, 0.25};
        double[] ex = {0.03125, 0.125, 0.21875, 0.25, 0.21875, 0.125, 0.03125};
        double[] act = psd.convolve(x, f);
        Assert.assertArrayEquals(ex, act, 0.001);

        double[] x2 = {0.08333, 0.16667, 0.16667, 0.16667, 0.16667, 0.16667, 0.08333};
        double[] f2 = {0.0625, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.0625};
        double[] ex2 = {0.005208, 0.020833, 0.041667, 0.0625, 0.083333, 0.104167, 0.119792, 0.125, 0.119792, 0.104167, 0.083333, 0.0625, 0.041667, 0.020833, 0.005208};
        act = psd.convolve(x2, f2);
        Assert.assertArrayEquals(ex2, act, 0.001);
    }

    @Test
    public void testRun() throws ExecException {
        DataBag inputBag = bagFactory.newDefaultBag();

        for (int i = 0; i < 50; i++) {
            Tuple t = tupleFactory.newTuple();
            t.append(i);
            t.append((double) i);
            inputBag.add(t);
        }

        Tuple input = tupleFactory.newTuple();
        input.append(inputBag);
        input.append(1);
        input.append(0.1);
        input.append(3);
        input.append(5);

        psd.exec(input);
    }
}
