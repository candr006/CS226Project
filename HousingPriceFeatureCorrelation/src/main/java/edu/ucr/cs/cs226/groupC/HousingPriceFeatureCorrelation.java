package edu.ucr.cs.cs226.groupC;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.stat.Correlation;


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

        //Correlation Matrix
        Dataset<Row> corr_matrix= Correlation.corr(boston_csv, "SalePrice");
        corr_matrix.show();



        sc.stop();
    }
}

