/* 
 * MAFReader - pipeline stage that reads MAF data into a spark Dataset
 */

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

public class MAFReader implements Serializable {

	private static final long serialVersionUID = 1L;
	SparkSession spark;
	String inputFileLocation;
	String labelFileLocation;
	String neoantigenFileLocation;
	String delimiter = ",";
	Boolean header = true;
	Integer neoepitopeLength = 4;
	Integer neoantigenLength = 9;
	Boolean balance = true;
	
	private HashMap<String, Patient> patientMap;
	private String PHYSICOCHEMICAL = "pc";
	private String BLOSUM = "bl";
	private String clusterType = PHYSICOCHEMICAL;
	
	/* Control flow booleans */
	private Boolean removeWildtypes = true;
	private Boolean clustersFromNeoepitopes = true;
	
	public MAFReader(SparkSession spark, String inputFileLocation, String labelFileLocation, String neoantigenFileLocation) {
		this.spark = spark;
		this.inputFileLocation = inputFileLocation;		
		this.labelFileLocation = labelFileLocation;
		this.neoantigenFileLocation = neoantigenFileLocation;
		patientMap = new HashMap<String, Patient>(); // {id : Patient}
	}
	
	/* 
	 * readMAF() - uses appropriate functions to add Mutations, Labels, and Neoantigens to each patient
	 * 	and returns processed dataset
	 */
	public Dataset<Patient> readMAF() {
		
		readCSVtoPatientMap();
		addLabels();
		addNeoantigens();
		
		return processPatients();
	}

	public List<Patient> getPatients() {
		List<Patient> patients = new ArrayList<Patient>(patientMap.values());
		return patients;
	}

	public Dataset<Patient> addMorePatients(String addedInputFileLocation, String addedLabelFileLocation) {
		
		System.err.println("INFO: Adding more patients.");
		
		addAdditionalPatients(addedInputFileLocation);
		addAdditionalLabels(addedLabelFileLocation);

		return processPatients();
	}

	/*
	 * processPatients - performs any processing functions on patients (i.e. extract neoepitopes, remove wildtypes, etc.) 
	 * and returns encoded data
	 */
	private Dataset<Patient> processPatients() {
		
		if (this.balance) {
			balancePatients();
		}
		
		List<Patient> patients = new ArrayList<Patient>(patientMap.values());
		
		if (this.removeWildtypes) {
			patients = removeWildtype(patients);
		}
		
		if (this.clustersFromNeoepitopes) {
			patients = extractNeoepitopes(patients);
//			patients = filterNeoepitopes(patients);
			patients = extractClustersFromNeoepitopes(patients);
		} else {
			patients = extractClustersFromNeoantigens(patients);
		}
		
		Encoder<Patient> patientEncoder = Encoders.bean(Patient.class);
		Dataset<Patient> data = spark.createDataset(
				patients,
				patientEncoder
				);
		return data;	
	}
	
	private void balancePatients() {
		
		System.err.println("Before balancing: " + patientMap.keySet().size());
		
		Double label;
		String patid;
		List<Patient> newPatients = new ArrayList<Patient>();
		for (Patient patient: this.patientMap.values()) {
			label = patient.getLabel();
			patid = patient.getId();
			if (label == 1.0 && !patid.contains("_2")) {
				Patient newPatient = patient.copy();
				String newId = newPatient.getId() + "_2";
				newPatient.setId(newId);
				newPatients.add(newPatient);
			}
		}
		
		for (Patient patient: newPatients) {
			this.patientMap.put(patient.getId(), patient);
		}
		
		System.err.println("After balancing: " + patientMap.keySet().size());

	}

	/*
	 * updateTargets - changes to sparse vector of single cluster counts
	 */
	public Dataset<Patient> updateTargets(String targetCluster) {
		List<Patient> patients = new ArrayList<Patient>(patientMap.values());
		List<String> targets;
		
		for (Patient patient: patients) {
			targets = PatientUtils.getClusters(patient, targetCluster);
			patient.setTargets(targets);
		}
		
		Encoder<Patient> patientEncoder = Encoders.bean(Patient.class);
		Dataset<Patient> data = spark.createDataset(
				patients,
				patientEncoder
				);
		return data;
	}

	
	/*
	 * filterNeoepitopes - given a text file of neoepitopes, filter out all other neoepitopes
	 */
	private List<Patient> filterNeoepitopes(List<Patient> patients) {
		System.err.println("INFO: Filtering neoepitopes");
		String path = "/Users/allipine/Desktop/ResearchML/Output/informative_neoepitopes.txt";
		List<String> informative_neoepitopes = new ArrayList<String>();
		BufferedReader br = null;
		String line = "";

		try {
			br = new BufferedReader(new FileReader(path));
			while ((line = br.readLine()) != null) {
				String[] data = line.replaceAll(" ", "").split(",");
				informative_neoepitopes = Arrays.asList(data);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		for (Patient patient:patients) {
			List<String> new_neoepitopes = new ArrayList<String>();
			List<String> neoepitopes = patient.getNeoepitopes();

			for (String neoepitope:neoepitopes) {
				if (informative_neoepitopes.contains(neoepitope)) {
					new_neoepitopes.add(neoepitope);
				}
			}
			patient.setNeoepitopes(new_neoepitopes);
			
		}

		
		return patients;
	}

	private void readCSVtoPatientMap() {
		
		BufferedReader br = null;
		String line = "";
		Patient patient = null;

		try {

			br = new BufferedReader(new FileReader(this.inputFileLocation));
			if (this.header) {
				br.readLine();
			}
			while ((line = br.readLine()) != null) {

				/* Reading a CSV */
				String[] patientData = line.split(this.delimiter);
				String patientId = patientData[0];
				String mutation = patientData[1];
				String cDNAchange = patientData[8];
				
				/* Skip non-protein coding genes */
				if (cDNAchange.equals("")) {
					continue;
				}

				if (mutation.equals("Unknown")) {
					continue;
				}
				
				if (patientMap.containsKey(patientId)) {
					patient = patientMap.get(patientId); 
					patient.addMutation(mutation);
				} else {
					patient = new Patient();
					patient.setId(patientId);
					patient.addMutation(mutation);
					patientMap.put(patientId, patient);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
	
	private void addLabels() {

		BufferedReader br = null;
		String line = "";
		Patient patient = null;		
		
		try {

			br = new BufferedReader(new FileReader(this.labelFileLocation));
			if (this.header) {
				br.readLine();
			}
			while ((line = br.readLine()) != null) {

				/* Reading label CSV */
				String[] labelData = line.split(this.delimiter);
				String patientId = labelData[0];	
				String response = labelData[7];
				Double label = 0.0;
				
				if (!response.equals("nonresponse")) {
					label = 1.0;
				}

				if (patientMap.containsKey(patientId)) {
					patient = patientMap.get(patientId);
					patient.setLabel(label);
				} 
				
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/* 
	 * removeWildtype - sets neoantigens to (neoantigens - wtantigens)
	 * 	- implementation: subtracts hash map for improved time complexity
	 */
	private List<Patient> removeWildtype(List<Patient> patients) {
		
		System.err.println("INFO: Removing wildtype antigens");
		
		List<String> neoantigens;
		List<String> wtantigens;
		
		for (Patient patient: patients) {
			neoantigens = patient.getNeoantigens();
			wtantigens = patient.getWtantigens();
			
			Map<String,String> map = new HashMap<String,String>();
			for (String antigen: wtantigens) {
				map.put(antigen, antigen);
			}
			
			List<String> subtracted = new ArrayList<String>();
			for (String neoantigen: neoantigens) {
				/* Only add a neoantigen if its not in the wildtype map */
				if (!neoantigen.equals(map.get(neoantigen))) {
					subtracted.add(neoantigen);
				} 
			}
			
			patient.setNeoantigens(subtracted);
		}
		return patients;
	}
	
	private List<Patient> extractClustersFromNeoepitopes(List<Patient> patients) {
		
		System.err.println("INFO: Extracting clusters");
		Map<String, String> clusters = createClusters();
		List<String> neoepitopes;
		
		for (Patient patient:patients) {
			neoepitopes = patient.getNeoepitopes();
			for (String neoepitope: neoepitopes) {
				String cluster = convertToCluster(neoepitope, clusters);
				if (cluster.length() != this.neoepitopeLength) {
					continue;
				}
				patient.addCluster(cluster);
			}
		}
		
		return patients;
	}
	
	private List<Patient> extractClustersFromNeoantigens(List<Patient> patients) {
		
		System.err.println("INFO: Extracting clusters");
		Map<String, String> clusters = createClusters();
		List<String> neoantigens;
		
		for (Patient patient:patients) {
			neoantigens = patient.getNeoantigens();
			for (String neoantigen: neoantigens) {
				String cluster = convertToCluster(neoantigen, clusters);
				if (cluster.length() != this.neoantigenLength) {
					continue;
				}
				patient.addCluster(cluster);
			}
		}
		
		return patients;
	}

	private String convertToCluster(String toConvert, Map<String, String> clusters) {
	
		String clustered = "";
		
		for (int i = 0; i < toConvert.length(); i++) {
			String key = Character.toString(toConvert.charAt(i));
			if (!clusters.containsKey(key)) {
//				System.err.println("Error: could not find amino acid symbol, " + key);
				return "";
			}
			clustered = clustered.concat(clusters.get(key));
		}
		
		return clustered;
	}
	
	private Map<String, String> createClusters() {
		
		Map<String, String> clusters = new HashMap<String, String>();
		
		if (this.clusterType.equals(this.PHYSICOCHEMICAL)) {
			
			/* Manually add all of the amino acids */
			clusters.put("F", "1");
			clusters.put("W", "4");
			clusters.put("Y", "3");
			clusters.put("C", "3");
			clusters.put("I", "4");
			clusters.put("L", "4");
			clusters.put("M", "4");
			clusters.put("V", "4");
			clusters.put("A", "4");
			clusters.put("T", "3");
			clusters.put("G", "4");
			clusters.put("P", "4");
			clusters.put("S", "3");
			clusters.put("D", "1");
			clusters.put("E", "1");
			clusters.put("N", "3");
			clusters.put("Q", "3");
			clusters.put("K", "2");
			clusters.put("R", "2");
			clusters.put("H", "2");
			clusters.put("U", "3"); // selenocysteine with cysteine
			clusters.put("B", "3"); // Glutamine/Glutamic go in Polar
			clusters.put("Z", "3"); // Asparagine/Aspartic go in Polar
			clusters.put("J", "4"); // Leucine/Isoleucine go in Nonpolar
			
		} else if (this.clusterType.equals(this.BLOSUM)) {
			
			clusters.put("F", "1");
			clusters.put("W", "1");
			clusters.put("Y", "1");
			clusters.put("C", "2");
			clusters.put("I", "2");
			clusters.put("L", "2");
			clusters.put("M", "2");
			clusters.put("V", "2");
			clusters.put("A", "3");
			clusters.put("T", "3");
			clusters.put("G", "4");
			clusters.put("P", "4");
			clusters.put("S", "4");
			clusters.put("D", "5");
			clusters.put("E", "5");
			clusters.put("N", "5");
			clusters.put("Q", "5");
			clusters.put("K", "5");
			clusters.put("R", "5");
			clusters.put("H", "6");
			clusters.put("U", "2"); // selenocysteine with cysteine
			clusters.put("B", "5");
			clusters.put("Z", "5");
			clusters.put("J", "2");
			
		} 
		
		return clusters;
	}

	private List<Patient> extractNeoepitopes(List<Patient> patients) {
		
		List<String> neoantigens;
		List<String> wtantigens;
 		
		for (Patient patient: patients) {
			neoantigens = patient.getNeoantigens();
			wtantigens = patient.getWtantigens();
			for (String neoantigen: neoantigens) {
				List<String> neoepitopes = getNeoepitopes(neoantigen);
				for (String neoepitope: neoepitopes) {
					patient.addNeoepitope(neoepitope);
				}
			}
			for (String wtantigen: wtantigens) {
				List<String> wtepitopes = getNeoepitopes(wtantigen);
				for (String wtepitope: wtepitopes) {
					patient.addWtepitope(wtepitope);
				}
			}
		}
		
		return patients;
	}
	
	private List<String> getNeoepitopes(String neoantigen) {
		
		List<String> neoepitopes = new ArrayList<String>();
		String neoepitope = "";
		Integer begin, end;
		Integer i = 0;
		
		while (this.neoepitopeLength + i <= neoantigen.length()) {
			
			begin = i;
			end = this.neoepitopeLength + i;
			neoepitope = neoantigen.substring(begin, end);
			neoepitopes.add(neoepitope);
			
			i++;
		}
		
		return neoepitopes;
	}

	private void addNeoantigens() {

		BufferedReader br = null;
		String line = "";
		Patient patient = null;		
		
		try {

			br = new BufferedReader(new FileReader(this.neoantigenFileLocation));
			if (this.header) {
				br.readLine();
			}
			while ((line = br.readLine()) != null) {

				/* Reading label CSV */
				String[] neoantigenData = line.split(this.delimiter);
				String patientId = neoantigenData[0];	
				String neoantigen = neoantigenData[10];
				String peptidePos = neoantigenData[7];
				String wtantigen = neoantigenData[13];

				if (peptidePos.equals("frameshift")) {
					neoantigen = neoantigenData[11]; 
					wtantigen = "";
				}
				
				if (patientMap.containsKey(patientId)) {
					patient = patientMap.get(patientId);
					patient.addNeoantigen(neoantigen);
					if (!wtantigen.equals("")) {
						patient.addWtantigen(wtantigen);
					}
				} 
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}		
		
	}
	
	private void addAdditionalPatients(String addedInputFileLocation) {
		BufferedReader br = null;
		String line = "";
		Patient patient = null;	
		
		try {

			br = new BufferedReader(new FileReader(addedInputFileLocation));
			if (this.header) {
				br.readLine();
			}
			while ((line = br.readLine()) != null) {

				/* Reading a CSV */
				String[] patientData = line.split(this.delimiter);
				String patientId = patientData[0];
				String mutation = patientData[1];
				String neoantigen = patientData[13];
				String wtantigen = patientData[14];
				neoantigen = neoantigen.toUpperCase();
				wtantigen = wtantigen.toUpperCase();
				
				/* Skip non-protein coding genes */
				if (neoantigen.equals("NA")) {
					continue;
				}

				if (neoantigen.contains("\"")) {
					System.err.println(line);;
				}
				
				if (mutation.equals("Unknown")) {
					continue;
				}
				
				if (patientMap.containsKey(patientId)) {
					patient = patientMap.get(patientId); 
					patient.addMutation(mutation);
					patient.addNeoantigen(neoantigen);
					if (!wtantigen.equals("NA")) {
						patient.addWtantigen(wtantigen);
					}
				} else {
					patient = new Patient();
					patient.setId(patientId);
					patient.addMutation(mutation);
					patient.addNeoantigen(neoantigen);
					if (!wtantigen.equals("NA")) {
						patient.addWtantigen(wtantigen);
					}
					patientMap.put(patientId, patient);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

	private void addAdditionalLabels(String addedLabelFileLocation) {
		BufferedReader br = null;
		String line = "";
		Patient patient = null;	
		
		try {

			br = new BufferedReader(new FileReader(addedLabelFileLocation));
			if (this.header) {
				br.readLine();
			}
			while ((line = br.readLine()) != null) {

				/* Reading label CSV */
				String[] labelData = line.split(this.delimiter);
				String patientId = labelData[0];	
				String response = labelData[12];
				Double label = Double.parseDouble(response);
				
				if (label != 0.0 && label != 1.0) {
					System.err.println("ERROR incorrect label in :" + line);
					continue;
				}

				if (patientMap.containsKey(patientId)) {
					patient = patientMap.get(patientId);
					patient.setLabel(label);
				} 
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/*** Getter and Setter Functions ***/
	public void setNeoepitopeLength(Integer len) {
		this.neoepitopeLength = len;
	}
	
	public Integer getNeoepitopeLength() {
		return this.neoepitopeLength;
	}
	
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setHeader(Boolean header) {
		this.header = header;
	}

	public Boolean hasHeader() {
		return header;
	}
	
	public void setClusterType(String clusterType) {
		if (!clusterType.equals(BLOSUM) && !clusterType.equals(PHYSICOCHEMICAL)) {
			System.err.println("ERROR: cluster type must be BLOSUM (bl) or PHYSICOCHEMICAL (pc)");
		} else {
			this.clusterType = clusterType;
		}
	}
	/*************************************/
}