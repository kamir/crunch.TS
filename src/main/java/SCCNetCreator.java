
import org.apache.crunchts.CrunchTSApp;
import org.apache.crunchts.simple.CombineTimeSeriesPairsAndTriplesFromTSBucket;
import org.apache.hadoop.util.ToolRunner;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author kamir
 */
public class SCCNetCreator {
    
      public static void main(String[] argv) throws Exception {

          String[] a = {"/TSBASE/Exp001/bucketA.tsb.vec.seq", "/TSBASE/Exp001/netA" };
          CombineTimeSeriesPairsAndTriplesFromTSBucket.main( a );
          
      }
    
}
