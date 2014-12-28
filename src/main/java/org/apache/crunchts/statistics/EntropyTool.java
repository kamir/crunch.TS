/**
 *   Source:    http://whaticode.com/2010/05/24/a-java-implementation-for-shannon-entropy/
 **/
package org.apache.crunchts.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author kamir
 */
public class EntropyTool {

    public static Double calculateShannonEntropy(double[] y1) {
        double entr = 0.0;
        List<String> v = new ArrayList<String>();
        for( double y: y1 ) {
            String p = y+"";
            v.add(p);
        }
        entr = calculateShannonEntropy( v );
        return entr;
    }

    /**
     * Time series data for a discrete equidistant time series
     * 
     * @param y1 - shorter time series
     * @param y2
     * @return 
     */
    public static Double calculateShannonEntropy(double[] y1, double[] y2) {
        double entr = 0.0;
        int i = 0;
        List<String> v = new ArrayList<String>();
        for( double y: y1 ) {
            String p = y + " " + y2[i];
            v.add(p);
            i++;
        }
        entr = calculateShannonEntropy( v );
        return entr;
    }
    
    private static Double calculateShannonEntropy(List<String> values) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        // count the occurrences of each value
        for (String sequence : values) {
            if (!map.containsKey(sequence)) {
                map.put(sequence, 0);
            }
            map.put(sequence, map.get(sequence) + 1);
        }

        // calculate the entropy
        Double result = 0.0;
        for (String sequence : map.keySet()) {
            Double frequency = (double) map.get(sequence) / values.size();
            result -= frequency * (Math.log(frequency) / Math.log(2));
        }

        return result;
    }

}
