
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;

public class PatientUtils {
	
	
	/* 
	 * Takes patient Dataset and returns a list of all possible clusters
	 */
	public static List<String> getAllClusters(Dataset<Patient> data) {
		
		System.err.println("INFO: Getting all the clusters.");
		
		List<Patient> patients = data.collectAsList();
		
		Set<String> clusters = new HashSet<String>();
		for (Patient patient: patients) {
			clusters.addAll(patient.getClusters());
		}
		List<String> allPossibleClusters = new ArrayList<String>(clusters);
		System.err.println("INFO: Retrieved " + allPossibleClusters.size() + " clusters.");
		
		return allPossibleClusters;
	}
	
	/*
	 * Given a cluster and a patient, return a list of only those clusters in the patient
	 */
	public static List<String> getClusters(Patient patient, String target) {
		
		List<String> clusters = patient.getClusters();
		List<String> targets = new ArrayList<String>();
		
		for (String cluster: clusters) {
			if (cluster.equals(target)) {
				targets.add(cluster);
			}
		}
		return targets;
	}
	
}