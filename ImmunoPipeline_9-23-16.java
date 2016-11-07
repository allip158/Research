/*
 * ImmunoPipeline - main class for running the machine learning pipeline
 */

/* TODO: cross validation
 * TODO: config file will likely contain specifications of pipeline 
 * i.e. model type, features, cross-validation (number folds), evaluation schema, ... 
 */

import java.util.Properties;
import java.io.InputStream;
import java.io.FileNotFoundException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;

public class ImmunoPipeline {

	private SparkSession spark;
	private String inputFileLocation;
	private String outputFileLocation;
	private String inputFileType;
	private String labelsFileLocation;
	private String neoantigenFileLocation;
	private Integer numberOfFolds = 5;
	
	private Dataset<Patient> training;
	private Dataset<Patient> testing;
	private int vocabSize = 1000; // 30000 max

	/* Constructor */
	public ImmunoPipeline(SparkSession spark) {
		this.spark = spark;
		setPropertiesFromConfigFile();
	}

	/* Main function to run the pipeline */
	public void run() {

		if (!this.inputFileType.equals("MAF")) {
			System.err.println("Unknown file type. Please use MAF.");
			System.exit(0);
		}

		/* Step 1: read training/testing data as Dataset */
		MAFReader mafr = new MAFReader(this.spark, this.inputFileLocation, this.labelsFileLocation, this.neoantigenFileLocation);
		System.err.println("Reading File...");
		Dataset<Patient> data = mafr.readMAF();
		data.show(5, true);
				
		Dataset<Patient>[] splits = data.randomSplit(new double[]{0.6, 0.4});//, 1234L);
		this.training = splits[0];
		this.testing = splits[1];
		
		Dataset<Row> predictions = trainAndTest();
		
		Evaluator evaluator = new BinaryClassificationEvaluator()
				.setLabelCol("label")
				.setMetricName("areaUnderPR") // other option: areaUnderROC
				.setRawPredictionCol("rawPrediction");
		double result = evaluator.evaluate(predictions);
		System.out.println("areaUnderPR = " + result);	
	}
	
	private Dataset<Row> trainAndTest() {
		
		/* Step 2: convert MAF to feature vector */
		CountVectorizerModel featureModel = new CountVectorizer()
				.setInputCol("neoepitopes")
				.setOutputCol("features")
				.fit(this.training);
		
		/* Step 3: train model using pipelines*/
		RandomForestClassifier classifier = new RandomForestClassifier()
				.setLabelCol("label")
				.setFeaturesCol("features");
		
		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] {featureModel, classifier});

		
		System.err.print("Building model...");
		PipelineModel model = pipeline.fit(this.training);

		/* Step 4: use model to predict on testing data and evaluate */
		System.err.println("DONE.\nMaking predictions...");
		Dataset<Row> predictions = model.transform(this.testing);
		predictions.show(true);
		
		return predictions;	
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
			this.numberOfFolds = Integer.parseInt(prop.getProperty("numberOfFolds", "5"));
			this.neoantigenFileLocation = prop.getProperty("neoantigenFileLocation");
			this.outputFileLocation = prop.getProperty("outputFileLocation");

		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

}