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
}
