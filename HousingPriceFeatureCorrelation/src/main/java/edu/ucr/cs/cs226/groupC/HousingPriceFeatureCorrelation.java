package edu.ucr.cs.cs226.groupC;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class HousingPriceFeatureCorrelation {
    public static void main(String[] args) {
// configure spark

        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> data = sc.textFile("boston_input_file.csv");
        System.out.println(data.toString());

        sc.stop();
    }
}

