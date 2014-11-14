package wikipedia;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Based on a filename like "pagecounts-20071210-010000.gz"
 * the time in millis is calculated.
 *
 * @author root
 */
public class TimeStampTool {

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
