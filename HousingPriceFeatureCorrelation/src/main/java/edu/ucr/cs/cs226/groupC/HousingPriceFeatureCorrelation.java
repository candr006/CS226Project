package edu.ucr.cs.cs226.groupC;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameStatFunctions;
import org.apache.spark.mllib.stat.correlation.Correlation.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.mllib.stat.correlation.Correlations.corr;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import org.apache.spark.sql.functions.*;
import scala.Tuple2;


public class HousingPriceFeatureCorrelation {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> data = sc.textFile("no_header_boston_input_file_numeric_only.csv");

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
        int i=1;
        JavaRDD<Tuple2<Double,Double>> ListArea = data.map(
                (String line) ->{
                    String[] fields = line.split(",");

                    if(Double.valueOf(fields[2]) instanceof Double) {
                        Double d = 0.0;
                        Double d2 = 0.0;
                        d = Double.valueOf(fields[2]);
                        d2 = Double.valueOf(fields[41]);
                        return new Tuple2<Double, Double>(d, d2);
                    }

                    return new Tuple2<Double, Double>(0.0,0.0);
                }

        );

      /*  JavaRDD<Tuple2<Double,Double>> ListArea2 = ListArea.filter(
                (Tuple2<Double,Double> t) ->
                {

                    return t!=ListArea.first();

                }

                );*/


        List<Double> list1 = Arrays.asList(ListArea.first()._1);

       List<Double> list2 = Arrays.asList(ListArea.first()._2);



        JavaDoubleRDD seriesX = sc.parallelizeDoubles(list1);

        JavaDoubleRDD seriesY = sc.parallelizeDoubles(list2);



        Double correlation = Statistics.corr(seriesX.srdd(),seriesY.srdd());

        System.out.println("Correlation is: " + correlation);


        //Correlation between columns?

       // Dataset<Row> correlated= Correlation.corr(boston_csv, "SalePrice", "pearson");
        //correlated.show();

        sc.stop();
    }
}

