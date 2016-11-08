/*
 * ImmunoPipeline - main class for running the machine learning pipeline
 */

/* TODO: Print settings in output file
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.io.InputStream;
import java.io.Serializable;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class ImmunoPipeline implements Serializable {

	private static final long serialVersionUID = 1L;
	private SparkSession spark;
	private String inputFileLocation;
	private String outputFileLocation;
	private String inputFileType;
	private String labelsFileLocation;
	private String neoantigenFileLocation;
	private String clusterType;
	
	private Map<String, List<Double>> results;
	private List<Double> auPRs;
	private Double split = 0.7;
	private Integer numIterationsSVM = 100;
	private Integer numRuns = 5;
	private Boolean writeResults = true;
	private MAFReader mafr;
	
	
	/* Constructor */
	public ImmunoPipeline(SparkSession spark) {
		this.spark = spark;
		setPropertiesFromConfigFile();
		this.mafr = new MAFReader(this.spark, this.inputFileLocation, this.labelsFileLocation, this.neoantigenFileLocation);
		this.results = new HashMap<String, List<Double>>();
		this.auPRs = new ArrayList<Double>();
	}

	/* Main function to run the pipeline */
	public void run() {

		Dataset<Patient> data = readData();
		
		/* Generate feature vectors from neoepitopes */
		CountVectorizerModel featureModel = new CountVectorizer()
				.setInputCol("clusters")
				.setOutputCol("counts")
				.fit(data);
		
		Dataset<Row> dataWithFeatures = featureModel.transform(data).select(col("label"), col("counts"));
		dataWithFeatures.show(5);
		
		/* Normalize each Vector using $L^1$ norm. */
//		Normalizer normalizer = new Normalizer()
//		  .setInputCol("counts")
//		  .setOutputCol("normFeatures")
//		  .setP(1.0);
//		
//		Dataset<Row> l1NormData = normalizer.transform(dataWithFeatures);
//		l1NormData = l1NormData.select(col("label"), col("normFeatures"));
//		l1NormData.show(5);
//		
		PCAModel pca = new PCA()
				  .setInputCol("counts")
				  .setOutputCol("features")
				  .setK(500)
				  .fit(dataWithFeatures);
		
		Dataset<Row> result = pca.transform(dataWithFeatures);
		
		Dataset<Row> processedData = result.select(col("label"), col("features"));

		processedData.show(5);
		
		trainAndTest(processedData);
		
		if (this.writeResults) {
			writeResults();
		} else {
			printResults();
		}
	}

	private Dataset<Patient> readData() {
		
		if (!this.inputFileType.equals("MAF")) {
			System.err.println("Unknown file type. Please use MAF.");
			System.exit(0);
		}

		/* Read training/testing data as Dataset */
		mafr.setClusterType(this.clusterType);
		System.err.println("Reading File...");
		Dataset<Patient> data = mafr.readMAF();
		
		/* Adding additional data */
		String SnyderDataLocation = "/Users/allipine/Desktop/ResearchML/Data/Snyder-NEJM-BindersMAF-noHLA.csv";
		String SnyderLabelLocation = "/Users/allipine/Desktop/ResearchML/Data/Snyder_Clinical.csv";
		data = mafr.addMorePatients(SnyderDataLocation, SnyderLabelLocation);
		
		data.show(5, true);
		
		return data;
	}
	
	private void trainAndTest(Dataset<Row> dataWithFeatures) {
		JavaRDD<LabeledPoint> rddData = dataWithFeatures.toJavaRDD().map(new MakeLabeledPointDenseRDD()); 
		
		JavaRDD<Tuple2<Object, Object>> scoreAndLabels;
		
		for (int i = 0; i < this.numRuns; i++) {
			
			/* Train and Test on data with features */
			scoreAndLabels = train(rddData);
			
			/* Evaluate and save results */
			evaluate(scoreAndLabels, i);
		}
		
	}
	
	private void writeResults() {
		
		try {

			File file = new File(this.outputFileLocation);

			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			String resultString = "";
			
			for (int i = 0; i < this.auPRs.size(); i++) {
				bw.write(i + ",AUPR," + this.auPRs.get(i) + "\n");
			}
			
			for (String key: this.results.keySet()) {
				bw.write(key + ",");
				resultString = this.results.get(key).toString();
				resultString = resultString.replaceAll("\\[", "");
				resultString = resultString.replaceAll("\\]", "");
				bw.write(resultString + "\n");
			}
			
			bw.close();

			System.out.println("Saved results to: " + this.outputFileLocation);

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void printResults() {
		
		for (int i = 0; i < this.auPRs.size(); i++) {
			System.err.println(i + ",AUPR," + this.auPRs.get(i) + "\n");
		}
		
	}
	
	/*
	 * train - takes a random sample of data and trains a SVM 
	 * WARN: column names must be "label" and "features"
	 */
	private JavaRDD<Tuple2<Object, Object>> train(JavaRDD<LabeledPoint> rddData) {

		JavaRDD<LabeledPoint> training = rddData.sample(false, this.split);
//		training.cache();
		JavaRDD<LabeledPoint> test = rddData.subtract(training);
		
		final SVMModel model = SVMWithSGD.train(training.rdd(), this.numIterationsSVM);

		JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
		  new Function<LabeledPoint, Tuple2<Object, Object>>() {
			  
		    public Tuple2<Object, Object> call(LabeledPoint p) {
		      Double score = model.predict(p.features());
		      return new Tuple2<Object, Object>(score, p.label());
		    }
		    
		  }
		);	
		
		return scoreAndLabels;
	}
	
	private void evaluate(JavaRDD<Tuple2<Object, Object>> scoreAndLabels, Integer run) {
		List<Double> predictions = new ArrayList<Double>();
		List<Double> labels = new ArrayList<Double>();
		
		
		for (Tuple2<Object, Object> tuple: scoreAndLabels.collect()) {
			predictions.add(tuple._1$mcD$sp());
			labels.add(tuple._2$mcD$sp());
		}

		results.put((run + ",predictions"), predictions);
		results.put((run + ",labels"), labels);
				
		// Get evaluation metrics.
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
		
		double auPR = metrics.areaUnderPR();
		this.auPRs.add(auPR);		

	}

	public static class MakeLabeledPointRDD implements Function<Row, LabeledPoint> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public LabeledPoint call(Row r) throws Exception {
		    SparseVector sparseFeatures = r.getAs(1); //keywords in RDD
		    Vector features = sparseFeatures.compressed();
		    Double label = r.getDouble(0); //id in RDD
		    LabeledPoint lp = new LabeledPoint(label, Vectors.fromML(features));
		    return lp;
		}		
	}
	
	public static class MakeLabeledPointDenseRDD implements Function<Row, LabeledPoint> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public LabeledPoint call(Row r) throws Exception {
		    DenseVector denseFeatures = r.getAs(1); //keywords in RDD
		    Vector features = denseFeatures.compressed();
		    Double label = r.getDouble(0); //id in RDD
		    LabeledPoint lp = new LabeledPoint(label, Vectors.fromML(features));
		    return lp;
		}		
	}
	

	private void setPropertiesFromConfigFile() {

		try {
			Properties prop = new Properties();
			String propFilename = "config.properties";

			InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFilename); 
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException(propFilename + " not found in class path.");
			}

			/* Set properties */
			this.inputFileLocation = prop.getProperty("inputFileLocation");
			this.inputFileType = prop.getProperty("inputFileType");
			this.labelsFileLocation = prop.getProperty("labelsFileLocation");
			this.numRuns = Integer.parseInt(prop.getProperty("numberOfRuns", "5"));
			this.neoantigenFileLocation = prop.getProperty("neoantigenFileLocation");
			this.outputFileLocation = prop.getProperty("outputFileLocation");
			this.clusterType = prop.getProperty("clusterType", "pc");

		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

}