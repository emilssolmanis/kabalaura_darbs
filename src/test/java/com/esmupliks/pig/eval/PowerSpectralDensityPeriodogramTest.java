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
}
