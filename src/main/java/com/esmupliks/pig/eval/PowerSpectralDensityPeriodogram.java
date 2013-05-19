package com.esmupliks.pig.eval;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
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

public class PowerSpectralDensityPeriodogram extends EvalFunc<DataBag> {
    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    public void demean(double[] data) {
        double m = StatUtils.mean(data);
        for (int i = 0; i < data.length; i++) {
            data[i] -= m;
        }
    }

    /** Copies the behavior of spec.taper in R.
     */
    public void taper(double[] data, double taper) {
        int numFromEnds = (int) Math.round(Math.floor(taper * data.length));
        double[] w = new double[numFromEnds];
        // init to 1, 3, 5 ...
        for (int i = 0; i < numFromEnds; i++) {
            w[i] = 2 * (i + 1) - 1;
        }
        for (int i = 0; i < w.length; i++) {
            w[i] = 0.5 * (1 - Math.cos(Math.PI * w[i] / (2 * numFromEnds)));
        }
        // taper front
        for (int i = 0; i < w.length; i++) {
            data[i] *= w[i];
        }
        // taper back
        for (int i = 0; i < w.length; i++) {
            int dIdx = data.length - 1 - i;
            data[dIdx] *= w[i];
        }
    }

    /** Computes the 1D convolution of x with filter f
     * @param x An array of values to compute the convolution for
     * @param f A 1D filter to apply
     */
    public double[] convolute(double[] x, double[] f) {
        // TODO: this is absolutely terrible, but seems to work. Rewrite decently
        double[] out = new double[x.length + f.length - 1];

        for (int i = 0; i < out.length; i++) {
            int xStart = Math.max(0, i - f.length + 1);
            int xEnd = Math.min(i + 1, x.length);

            int len = xEnd - xStart;

            for (int j = 0; j < len; j++) {
                out[i] += x[xStart + j] * f[Math.min(i, f.length - 1) - j];
            }
        }

        return out;
    }

    private double[] singleDaniell(int len) {
        double[] filter = new double[len];

        for (int j = 0; j < filter.length; j++) {
            filter[j] = 1.0 / (len - 1);
        }

        filter[0] = 1.0 / (2 * (len - 1));
        filter[filter.length - 1] = 1.0 / (2 * (len - 1));
        return filter;
    }

    /** Assumes that the lens array is not empty
     */
    public double[] modifiedDaniell(int[] lens) throws ExecException {
        double[] filter = singleDaniell(lens[0]);
        for (int i = 1; i < lens.length; i++) {
            if (lens[i] % 2 == 0) {
                throw new ExecException("Daniell filter spans have to be odd numbers!");
            }
            double[] curr = singleDaniell(lens[i]);
            filter = convolute(filter, curr);
        }
        return filter;
    }

    /** Input parameters are expected to be (in this order):
     * <ul>
     * <li>a sorted bag of tuples to take the PSD of, assumed to have less than 2^31 elements</li>
     * <li>the field of the bag's tuples to take the PSD of</li>
     * <li>the cosine-bell taper percentage</li>
     * <li>1 or more modified Daniell filter sizes (odd integers) to apply</li>
     * </ul>
     */
    @Override
    public DataBag exec(Tuple input) throws ExecException {
        DataBag inputData = (DataBag) input.get(0);
        int origInputSize = (int) inputData.size();
        int field = (Integer) input.get(1);
        double taperParam = (Double) input.get(2);
        int[] daniell = new int[input.size() - 3];
        for (int i = 3; i < input.size(); i++) {
            daniell[i - 3] = (Integer) input.get(i);
        }

        double[] data = new double[origInputSize];
        int i = 0;
        for (Tuple t : inputData) {
            data[i] = (Double) t.get(i);
            ++i;
        }
        
        demean(data);
        taper(data, taperParam);

        FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
        Complex[] res = fft.transform(data, TransformType.FORWARD);
        int half = data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1;
        data = new double[half];
        for (i = 0; i < half; i++) {
            data[i] = Math.pow((half / 2) * (res[i].abs() / half), 2);
        }

        // TODO: use an FFT, the convolution method gets a bit noisy and adds spare data
        // points which we now have to remove
        double[] filter = modifiedDaniell(daniell);
        data = convolute(data, filter);
        int spare = (filter.length - 1) / 2;

        DataBag result = bagFactory.newDefaultBag();
        for (i = spare; i < data.length - spare; i++) {
            Tuple t = tupleFactory.newTuple();
            t.append(i);
            double frequency = (i - spare) / (double) origInputSize;
            t.append(frequency);
            t.append(data[i]);
            result.add(t);
        }

        // demean
        // apply Cosine bell taper
        // take FFT
        // get magnitudes
        // raise to square & multiply by n
        // apply modified Daniell filters to smoothen the curve
        return result;
    }

    @Override
    public Schema outputSchema(Schema inputSchema) {
        try {
            Schema bagSchema = new Schema();
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("idx", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("frequency", DataType.DOUBLE));
            tupleSchema.add(new FieldSchema("power", DataType.DOUBLE));
            bagSchema.add(new FieldSchema("sample", tupleSchema, DataType.TUPLE));
            return new Schema(new FieldSchema("psd", bagSchema, DataType.BAG));            
        } catch (FrontendException e) {
            return null;
        }
    }
}

