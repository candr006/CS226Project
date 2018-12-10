package edu.ucr.cs.cs226.groupC;

// import org.apache.spark.api.java.JavaDoubleRDD;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.ml.stat.Correlation;
// import org.apache.spark.mllib.linalg.Matrix;
// import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.stat.Statistics;
// import org.apache.spark.rdd.RDD;
// import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.DataFrameStatFunctions;
// import org.apache.spark.mllib.stat.correlation.Correlation.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.mllib.stat.correlation.Correlations.corr;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import org.apache.spark.sql.functions.*;
import scala.Tuple2;



import java.util.Arrays;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;


import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

public class HousingPriceFeatureCorrelation {

    public static void FrequentPattern(){
        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> data = sc.textFile("/no_header_boston_input_numeric_only.csv");

        JavaRDD<FPGrowth.FreqItemset<Double>> SalePriceFI = data.map(
                (String line) ->{
                    String[] fields = line.split(",");

                    return new FPGrowth.FreqItemset<>(new Double[] {Double.valueOf(fields[41])}, 15L);
                }

        );


        AssociationRules arules = new AssociationRules()
                .setMinConfidence(0.8);
        JavaRDD<AssociationRules.Rule<Double>> results = arules.run(SalePriceFI);

        for (AssociationRules.Rule<Double> rule : results.collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }


        sc.stop();

        return;
    }


    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext();
        sc.setLogLevel("WARN");

        JavaRDD<String> data = sc.textFile("/no_header_boston_input_numeric_only.csv");

        //schema
        StructType customSchema = customSchema = new StructType(new StructField[] {
                new StructField("Id",DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("MSSubClass",DataTypes.StringType, true, Metadata.empty()),
                new StructField("LotFrontage",DataTypes.StringType, true, Metadata.empty()),
                new StructField("LotArea",DataTypes.StringType, true, Metadata.empty()),
                new StructField("HouseStyle",DataTypes.StringType, true, Metadata.empty()),
                new StructField("OverallQual",DataTypes.StringType, true, Metadata.empty()),
                new StructField("OverallCond",DataTypes.StringType, true, Metadata.empty()),
                new StructField("YearBuilt",DataTypes.StringType, true, Metadata.empty()),
                new StructField("YearRemodAdd",DataTypes.StringType, true, Metadata.empty()),
                new StructField("MasVnrArea",DataTypes.StringType, true, Metadata.empty()),
                new StructField("BsmtFinSF1",DataTypes.StringType, true, Metadata.empty()),
                new StructField("BsmtFinSF2",DataTypes.StringType, true, Metadata.empty()),
                new StructField("BsmtUnfSF",DataTypes.StringType, true, Metadata.empty()),
                new StructField("TotalBsmtSF",DataTypes.StringType, true, Metadata.empty()),
                new StructField("CentralAir",DataTypes.StringType, true, Metadata.empty()),
                new StructField("1stFlrSF",DataTypes.StringType, true, Metadata.empty()),
                new StructField("2ndFlrSF",DataTypes.StringType, true, Metadata.empty()),
                new StructField("LowQualFinSF",DataTypes.StringType, true, Metadata.empty()),
                new StructField("GrLivArea",DataTypes.StringType, true, Metadata.empty()),
                new StructField("BsmtFullBath",DataTypes.StringType, true, Metadata.empty()),
                new StructField("BsmtHalfBath",DataTypes.StringType, true, Metadata.empty()),
                new StructField("FullBath",DataTypes.StringType, true, Metadata.empty()),
                new StructField("HalfBath",DataTypes.StringType, true, Metadata.empty()),
                new StructField("BedroomAbvGr",DataTypes.StringType, true, Metadata.empty()),
                new StructField("KitchenAbvGr",DataTypes.StringType, true, Metadata.empty()),
                new StructField("TotRmsAbvGrd",DataTypes.StringType, true, Metadata.empty()),
                new StructField("Fireplaces",DataTypes.StringType, true, Metadata.empty()),
                new StructField("GarageYrBlt",DataTypes.StringType, true, Metadata.empty()),
                new StructField("GarageCars",DataTypes.StringType, true, Metadata.empty()),
                new StructField("GarageArea",DataTypes.StringType, true, Metadata.empty()),
                new StructField("PavedDrive",DataTypes.StringType, true, Metadata.empty()),
                new StructField("WoodDeckSF",DataTypes.StringType, true, Metadata.empty()),
                new StructField("OpenPorchSF",DataTypes.StringType, true, Metadata.empty()),
                new StructField("EnclosedPorch",DataTypes.StringType, true, Metadata.empty()),
                new StructField("3SsnPorch",DataTypes.StringType, true, Metadata.empty()),
                new StructField("ScreenPorch",DataTypes.StringType, true, Metadata.empty()),
                new StructField("PoolArea",DataTypes.StringType, true, Metadata.empty()),
                new StructField("PoolQC",DataTypes.StringType, true, Metadata.empty()),
                new StructField("MiscVal",DataTypes.StringType, true, Metadata.empty()),
                new StructField("MoSold",DataTypes.StringType, true, Metadata.empty()),
                new StructField("YrSold",DataTypes.StringType, true, Metadata.empty()),
                new StructField("SalePrice",DataTypes.IntegerType,true, Metadata.empty())});

        //start the spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Housing Price Feature Correlation")
                .getOrCreate();



        //Load Boston csv to dataset
        /*Dataset<Row> boston_csv = spark.read()
                .format("csv")
                .option("header","true")
                .schema(customSchema)
                .load("boston_input_numeric_only.csv");*/


        // Double LotArea= data.getRows(1,1).select(col("LotArea"))
        JavaRDD<Double> salePrices = data.map(
                (String line) ->{
                    String[] fields = line.split(",");
                    Double d2 = 0.0;
                    d2 = Double.valueOf(fields[41]);
                    return d2;
                }

        );

        //Loop through all features and output correlation between the feature and Sale Price
        JavaRDD<Double> csv_column=null;
        Double correlation= null;
        String[] column_array=new String[]{"Id","MSSubClass","LotFrontage","LotArea","HouseStyle","OverallQual","OverallCond","YearBuilt","YearRemodAdd","MasVnrArea","BsmtSF1","BsmtSF2","BsmtSF","TotalBsmtSF","CentralAir","1stFlrSF","2ndFlrSF","LowQualSF","GrLivArea","BsmtFullBath","BsmtHalfBath","FullBath","HalfBath","BedroomAbvGr","KitchenAbvGr","TotRmsAbvGrd","Fireplaces","GarageYrBlt","GarageCars","GarageArea","PavedDrive","WoodDeckSF","OpenPorchSF","EnclosedPorch","3SsnPorch","ScreenPorch","PoolArea","PoolQC","MiscVal","MoSold","YrSold","SalePrice"};

        for(int i=1; i<(column_array.length-1); i++){
            String col_name=column_array[i];
            int finalI = i;
            csv_column = data.map(
                    (String line) -> {
                        String[] fields = line.split(",");
                        Double d = 0.0;
                        Double d2 = 0.0;
                        d = Double.valueOf(fields[finalI]);
                        return d;
                    }

            );


            correlation = Statistics.corr(csv_column, salePrices);
            System.out.println("Correlation between feature "+ col_name+" and Sale Price is: " + correlation);
        }




        //Correlation between columns?

        // Dataset<Row> correlated= Correlation.corr(boston_csv, "SalePrice", "pearson");
        //correlated.show();


        // Load and parse data
        String filePath = "/boston_input_numeric_only.csv";

        spark = SparkSession.builder()
                .master("local[8]")
                .appName("PCAExpt")
                .getOrCreate();

        // Loads data.
        Dataset<Row> inDataset = spark.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load(filePath);

        ArrayList<String> inputColsList = new ArrayList<String>(Arrays.asList(inDataset.columns()));

        //Make single features column for feature vectors
        inputColsList.remove("class");
        String[] inputCols = inputColsList.parallelStream().toArray(String[]::new);

        //Prepare dataset for training with all features in "features" column
        VectorAssembler assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features");
        Dataset<Row> dataset = assembler.transform(inDataset);

        PCAModel pca = new PCA()
                .setK(16)
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .fit(dataset);

        Dataset<Row> result = pca.transform(dataset).select("pcaFeatures");
        System.out.println("Explained variance:");
        System.out.println(pca.explainedVariance());
        result.show(false);
        System.out.println("count:" + result.count());

        spark = SparkSession.builder()
                .master("local[8]")
                .appName("Regression")
                .getOrCreate();

        JavaRDD<String> dataReg = sc.textFile("/boston_input_numeric_only.csv");
        JavaRDD<LabeledPoint> parsedData = dataReg.map(line -> {
            String[] parts = line.split(",");
            String[] features = parts[1].split(" ");
            double[] v = new double[features.length];
            for (int i = 0; i < features.length - 1; i++) {
                v[i] = Double.parseDouble(features[i]);
            }
            return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
        });
        parsedData.cache();

// Building the model
        int numIterations = 100;
        double stepSize = 0.00000001;
        LinearRegressionModel model =
                LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations, stepSize);

// Evaluate model on training examples and compute training error
        JavaPairRDD<Double, Double> valuesAndPreds = parsedData.mapToPair(point ->
                new Tuple2<>(model.predict(point.features()), point.label()));

        double MSE = valuesAndPreds.mapToDouble(pair -> {
            double diff = pair._1() - pair._2();
            return diff * diff;
        }).mean();
        System.out.println("training Mean Squared Error = " + MSE);

// Save and load model
        model.save(sc.sc(), "target/tmp/javaLinearRegressionWithSGDModel");
        LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(),
                "target/tmp/javaLinearRegressionWithSGDModel");


        spark.stop();

        sc.stop();
    }
}

