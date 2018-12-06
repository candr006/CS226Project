package edu.ucr.cs.cs226.groupC;

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

import static org.apache.spark.mllib.stat.correlation.Correlations.corr;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class HousingPriceFeatureCorrelation {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> data = sc.textFile("boston_input_file.csv");

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
        Dataset<Row> boston_csv = spark.read()
                .format("csv")
                .option("header","true")
                .schema(customSchema)
                .load("boston_input_file.csv");


        //Correlation between columns?
        Dataset<Row> correlated= Correlation.corr(boston_csv, "SalePrice", "pearson");
        correlated.show();

        sc.stop();
    }
}

