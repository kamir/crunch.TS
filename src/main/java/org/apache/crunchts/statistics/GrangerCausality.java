package org.apache.crunchts.statistics;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.FDistribution;
import org.apache.commons.math.distribution.FDistributionImpl;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;

/**
 * A simple Granger-Causality test in pure Java.
 */
public class GrangerCausality {

    /**
     * Returns the p-value for the Granger-Causality test.
     *
     * @param y - predictable variable
     * @param x - predictor
     * @param l - lag, should be 1 or greater.
     * @return p-value of Granger-Causality
     */
    public static double grangerCausalityTest(double[] y, double[] x, int l){
        OLSMultipleLinearRegression h0 = new OLSMultipleLinearRegression();
        OLSMultipleLinearRegression h1 = new OLSMultipleLinearRegression();

        double[][] laggedY = createLaggedSide(l, y);

        double[][] laggedXY = createLaggedSide(l, x, y);

        int n = laggedY.length;

        h0.newSampleData(strip(l, y), laggedY);
        h1.newSampleData(strip(l, y), laggedXY);

        double rs0[] = h0.estimateResiduals();
        double rs1[] = h1.estimateResiduals();


        double RSS0 = sqrSum(rs0);
        double RSS1 = sqrSum(rs1);

        double ftest = ((RSS0 - RSS1)/l) / (RSS1 / ( n - 2*l - 1));

        System.out.println(RSS0 + " " + RSS1);
        System.out.println("F-test " + ftest);

        FDistribution fDist = new FDistributionImpl(l, n-2*l-1);
        try {
            double pValue = 1.0 - fDist.cumulativeProbability(ftest);
            System.out.println("P-value " + pValue);
            return  pValue;
        } catch (MathException e) {
            throw new RuntimeException(e);
        }

    }


    private static double[][] createLaggedSide(int L, double[]... a) {
        int n = a[0].length - L;
        double[][] res = new double[n][L*a.length+1];
        for(int i=0; i<a.length; i++){
            double[] ai = a[i];
            for(int l=0; l<L; l++){
                for(int j=0; j<n; j++){
                    res[j][i*L+l] = ai[l+j];
                }
            }
        }
        for(int i=0; i<n; i++){
            res[i][L*a.length] = 1;
        }
        return res;
    }

    public static double sqrSum(double[] a){
        double res = 0;
        for(double v : a){
            res+=v*v;
        }
        return res;
    }


     public static double[] strip(int l, double[] a){

        double[] res = new double[a.length-l];
        System.arraycopy(a, l, res, 0, res.length);
        return res;
    }




}
