package de.bitocean.util.wikipedia;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Wikipedia Click-Count data is public available.
 * 
 * Each file contains aggregated hourly click-counts for all 
 * Wikipedia pages.
 * 
 * Based on the filename like "pagecounts-20071210-010000.gz"
 * the time stamp in milli seconds is calculated.
 * 
 * The timestamp represents the beginning of the hour for which data 
 * is provided. Due to inqaccuracy one can not relay on all digits.
 * 
 * This implementation does not do any kind of correction.
 * More advanced testing is required for future releases. 
 *
 * @author Mirko Kämpf
 * 
 */
public class TimeStampTool {

    /**
     * Extract time stamp from click-count file name.
     * 
     * @param filename
     * @return timestamp in ms
     */
    static public long getTimeInMillis(String filename) {
        // System.out.println( filename );
        String[] s = filename.split("-");
        int j = Integer.parseInt(s[1].substring(0,4));
        int m = Integer.parseInt(s[1].substring(4,6));
        int d = Integer.parseInt(s[1].substring(6,8));
        int h = Integer.parseInt(s[2].substring(0,2));
        Calendar cal = new GregorianCalendar(j, m-1, d, h,0);
        return cal.getTimeInMillis();
    }
    
    public static void main( String[] args ) { 
        String fn = "pagecounts-20071210-010000.gz";
        System.out.println( new Date( getTimeInMillis( fn ) ));
        System.out.println( getTimeInMillis( fn ) );
    }
    
    
}
