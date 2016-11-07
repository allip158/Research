/* 
 * MAFReader - pipeline stage that reads MAF data into a spark Dataset
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MAFReader {

	SparkSession spark;
	String inputFileLocation;
	String labelFileLocation;
	String delimiter = ",";
	Boolean header = true;
	private HashMap<String, Double> labelMap;

	public MAFReader(SparkSession spark, String inputFileLocation, String labelFileLocation) {
		this.spark = spark;
		this.inputFileLocation = inputFileLocation;		
		this.labelFileLocation = labelFileLocation;
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

	public Dataset<Patient> readMAF() {

		BufferedReader br = null;
		String line = "";
		Patient patient = null;
		String prevId = "";
		List<Patient> patients = new ArrayList<Patient>();
		createLabelMap();

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

				if (prevId.equals(patientId)) {
					patient.addMutation(mutation);

				} else {
					if (patient != null) {
						Double label = labelMap.get(patient.id);
						if (label != null) {
							patient.setLabel(label);
							patients.add(patient);
						}
					}
					patient = new Patient();
					patient.setId(patientId);
					patient.addMutation(mutation);

				}
				prevId = patient.getId();
			}
			if (patient != null) {
				Double label = labelMap.get(patient.id);
				if (label != null) {
					patient.setLabel(label);
					patients.add(patient);
				}
			}

			Encoder<Patient> patientEncoder = Encoders.bean(Patient.class);
			Dataset<Patient> data = spark.createDataset(
					patients,
					patientEncoder
					);
			return data;

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
		return null;

	}
	
	private void createLabelMap() {
		BufferedReader br = null;
		String line = "";
		labelMap = new HashMap<String, Double>();

		try {

			br = new BufferedReader(new FileReader(this.labelFileLocation));
			if (this.header) {
				br.readLine();
			}
			while ((line = br.readLine()) != null) {

				/* Reading a CSV */
				String[] labelData = line.split(this.delimiter);
				String patientId = labelData[0];	
				String response = labelData[1];
				Double label = 0.0;
				
				if (response.equals("response")) {
					label = 1.0;
				}

				labelMap.put(patientId, label);
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

}
