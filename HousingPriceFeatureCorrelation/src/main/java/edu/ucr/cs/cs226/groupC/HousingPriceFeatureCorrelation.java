package edu.ucr.cs.cs226.groupC;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.mllib.stat.*;
import org.apache.spark.sql.DataFrameStatFunctions;
import org.apache.spark.mllib.stat.correlation.Correlation.*;

import static org.apache.spark.mllib.stat.correlation.Correlations.corr;


public class HousingPriceFeatureCorrelation {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> data = sc.textFile("boston_input_file.csv");

        //start the spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Housing Price Feature Correlation")
                .getOrCreate();

        //Load Boston csv to dataset
        Dataset<Row> boston_csv = spark.read().format("csv").option("header","true").load("boston_input_file.csv");
        boston_csv= boston_csv.filter("PoolArea != ''");
        //Correlation between columns?
        Dataset<Row> correlated= Correlation.corr(boston_csv, "PoolArea", "pearson");
        correlated.show();


        sc.stop();
    }
}

