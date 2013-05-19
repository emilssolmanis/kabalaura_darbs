package com.esmupliks.pig.eval;

import org.junit.Assert;
import org.junit.Test;

public class PowerSpectralDensityPeriodogramTest {
    private PowerSpectralDensityPeriodogram psd = new PowerSpectralDensityPeriodogram();

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
    public void testConvolute() {
        double[] x = {0.125, 0.25, 0.25, 0.25, 0.125};
        double[] f = {0.25, 0.5, 0.25};
        double[] ex = {0.03125, 0.125, 0.21875, 0.25, 0.21875, 0.125, 0.03125};
        double[] act = psd.convolute(x, f);
        Assert.assertArrayEquals(ex, act, 0.001);

        double[] x2 = {0.08333, 0.16667, 0.16667, 0.16667, 0.16667, 0.16667, 0.08333};
        double[] f2 = {0.0625, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.0625};
        double[] ex2 = {0.005208, 0.020833, 0.041667, 0.0625, 0.083333, 0.104167, 0.119792, 0.125, 0.119792, 0.104167, 0.083333, 0.0625, 0.041667, 0.020833, 0.005208};
        act = psd.convolute(x2, f2);
        Assert.assertArrayEquals(ex2, act, 0.001);
    }
}
