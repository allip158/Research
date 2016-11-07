/*
 * immunoML Tester - main test to start spark session and run framework
 */


import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class immunoMLtester {
	
	public static void main(String[] args) {
		
		/* Mute INFO logging */
	    Logger.getLogger("org").setLevel(Level.OFF);
	    Logger.getLogger("akka").setLevel(Level.OFF);
		
	    /* Create SparkSession */
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("immunoML")
				.getOrCreate();	
	
		/* run */
		ImmunoPipeline pipeline;
		try {
			pipeline = new ImmunoPipeline(spark);
			pipeline.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
