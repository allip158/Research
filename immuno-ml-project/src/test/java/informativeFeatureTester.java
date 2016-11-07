/*
 * informativeFeatureTester - run framework for each cluster for most informative one
 */


import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class informativeFeatureTester {
	
	public static void main(String[] args) {
		
		/* Mute INFO logging */
	    Logger.getLogger("org").setLevel(Level.OFF);
	    Logger.getLogger("akka").setLevel(Level.OFF);
		
	    /* Create SparkSession */
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("informativeFeatures")
				.getOrCreate();	
	
		/* run */
		ImmunoPipeline pipeline;
		try {
			pipeline = new ImmunoPipeline(spark);
			pipeline.findInformativeFeatures();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
