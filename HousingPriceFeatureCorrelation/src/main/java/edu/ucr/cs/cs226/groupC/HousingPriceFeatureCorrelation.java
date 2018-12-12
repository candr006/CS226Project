package edu.ucr.cs.cs226.groupC;

// import org.apache.spark.api.java.JavaDoubleRDD;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.ml.stat.Correlation;
// import org.apache.spark.mllib.linalg.Matrix;
// import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.api.java.function.Function;
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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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


public class HousingPriceFeatureCorrelation {

    public static void FrequentPattern(){
        SparkConf conf = new SparkConf().setAppName("FP-Growth-ItemFrequency").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //      String Filename = "gt_300000.csv";// 87
        //      String Filename = "gt_200000.csv";//312
        //      String Filename = "gt_500000.csv";//5
        String Filename = "lt_100000.csv";//123
        //         String Filename = "gt_100000.csv";//910
//        String Filename = "Feature Frequency.csv";

        JavaRDD<String> data = sc.textFile(Filename);

        JavaRDD<List<String>> features = data.map(new Function<String, List<String>>() {
            @Override
            public List<String> call(String v1) throws Exception {
                String[] parts = v1.split(",");
                return Arrays.asList(parts);
            }
        });

        Function<String,Boolean> filterPredicate = e -> e.contains(Filename);

        JavaRDD<String> rdd = data.filter(filterPredicate);
        rdd.saveAsTextFile(Filename + ".csv");



        JavaRDD<String> textFile = sc.textFile("Price Frequency.csv");
        JavaPairRDD<String,Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word->new Tuple2<>(word,1)).reduceByKey((a,b) -> a + b);
        counts.saveAsTextFile("PriceRange.txt");

        FPGrowth fpg = new FPGrowth().setMinSupport(0.9).setNumPartitions(1);
        System.out.println("Now we set up a fpg.");

        FPGrowthModel<String> model = fpg.run(features);

        model.freqItemsets()
                .toJavaRDD()
                .map((Function<FPGrowth.FreqItemset<String>, String>) fi -> fi.javaItems() + " -> " + fi.freq())
                .saveAsTextFile("output" + Filename);

        sc.stop();

        return;
    }


    public static void main(String[] args) throws IOException {

        JavaSparkContext sc = new JavaSparkContext();
        sc.setLogLevel("WARN");

        JavaRDD<String> data = sc.textFile("no_header_boston_input_numeric_only.csv");

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
        FileWriter fileWriter = new FileWriter("Correlation_Output.csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);

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
           printWriter.print(col_name+","+correlation);
           //System.out.println("Correlation between feature "+ col_name+" and Sale Price is: " + correlation);
        }




        //Correlation between columns?

       // Dataset<Row> correlated= Correlation.corr(boston_csv, "SalePrice", "pearson");
        //correlated.show();


        // Load and parse data
        String filePath = "boston_input_numeric_only.csv";

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
        
        spark.stop();

        sc.stop();

        FrequentPattern();
    }
}

