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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.mllib.stat.correlation.Correlations.corr;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class HousingPriceFeatureCorrelation {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> data = sc.textFile("boston_input_file.csv");

        //start the spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Housing Price Feature Correlation")
                .getOrCreate();

        //schema
        StructType customSchema = customSchema = new StructType(new StructField[] {
                new StructField("Id",DataTypes.IntegerType, true, null),
                new StructField("MSSubClass",DataTypes.StringType, true, null),
                new StructField("MSZoning",DataTypes.StringType, true, null),
                new StructField("LotFrontage",DataTypes.StringType, true, null),
                new StructField("LotArea",DataTypes.StringType, true, null),
                new StructField("Street",DataTypes.StringType, true, null),
                new StructField("Alley",DataTypes.StringType, true, null),
                new StructField("LotShape",DataTypes.StringType, true, null),
                new StructField("LandContour",DataTypes.StringType, true, null),
                new StructField("Utilities",DataTypes.StringType, true, null),
                new StructField("LotConfig",DataTypes.StringType, true, null),
                new StructField("LandSlope",DataTypes.StringType, true, null),
                new StructField("Neighborhood",DataTypes.StringType, true, null),
                new StructField("Condition1",DataTypes.StringType, true, null),
                new StructField("Condition2",DataTypes.StringType, true, null),
                new StructField("BldgType",DataTypes.StringType, true, null),
                new StructField("HouseStyle",DataTypes.StringType, true, null),
                new StructField("OverallQual",DataTypes.StringType, true, null),
                new StructField("OverallCond",DataTypes.StringType, true, null),
                new StructField("YearBuilt",DataTypes.StringType, true, null),
                new StructField("YearRemodAdd",DataTypes.StringType, true, null),
                new StructField("RoofStyle",DataTypes.StringType, true, null),
                new StructField("RoofMatl",DataTypes.StringType, true, null),
                new StructField("Exterior1st",DataTypes.StringType, true, null),
                new StructField("Exterior2nd",DataTypes.StringType, true, null),
                new StructField("MasVnrType",DataTypes.StringType, true, null),
                new StructField("MasVnrArea",DataTypes.StringType, true, null),
                new StructField("ExterQual",DataTypes.StringType, true, null),
                new StructField("ExterCond",DataTypes.StringType, true, null),
                new StructField("Foundation",DataTypes.StringType, true, null),
                new StructField("BsmtQual",DataTypes.StringType, true, null),
                new StructField("BsmtCond",DataTypes.StringType, true, null),
                new StructField("BsmtExposure",DataTypes.StringType, true, null),
                new StructField("BsmtFinType1",DataTypes.StringType, true, null),
                new StructField("BsmtFinSF1",DataTypes.StringType, true, null),
                new StructField("BsmtFinType2",DataTypes.StringType, true, null),
                new StructField("BsmtFinSF2",DataTypes.StringType, true, null),
                new StructField("BsmtUnfSF",DataTypes.StringType, true, null),
                new StructField("TotalBsmtSF",DataTypes.StringType, true, null),
                new StructField("Heating",DataTypes.StringType, true, null),
                new StructField("HeatingQC",DataTypes.StringType, true, null),
                new StructField("CentralAir",DataTypes.StringType, true, null),
                new StructField("Electrical",DataTypes.StringType, true, null),
                new StructField("1stFlrSF",DataTypes.StringType, true, null),
                new StructField("2ndFlrSF",DataTypes.StringType, true, null),
                new StructField("LowQualFinSF",DataTypes.StringType, true, null),
                new StructField("GrLivArea",DataTypes.StringType, true, null),
                new StructField("BsmtFullBath",DataTypes.StringType, true, null),
                new StructField("BsmtHalfBath",DataTypes.StringType, true, null),
                new StructField("FullBath",DataTypes.StringType, true, null),
                new StructField("HalfBath",DataTypes.StringType, true, null),
                new StructField("BedroomAbvGr",DataTypes.StringType, true, null),
                new StructField("KitchenAbvGr",DataTypes.StringType, true, null),
                new StructField("KitchenQual",DataTypes.StringType, true, null),
                new StructField("TotRmsAbvGrd",DataTypes.StringType, true, null),
                new StructField("Functional",DataTypes.StringType, true, null),
                new StructField("Fireplaces",DataTypes.StringType, true, null),
                new StructField("FireplaceQu",DataTypes.StringType, true, null),
                new StructField("GarageType",DataTypes.StringType, true, null),
                new StructField("GarageYrBlt",DataTypes.StringType, true, null),
                new StructField("GarageFinish",DataTypes.StringType, true, null),
                new StructField("GarageCars",DataTypes.StringType, true, null),
                new StructField("GarageArea",DataTypes.StringType, true, null),
                new StructField("GarageQual",DataTypes.StringType, true, null),
                new StructField("GarageCond",DataTypes.StringType, true, null),
                new StructField("PavedDrive",DataTypes.StringType, true, null),
                new StructField("WoodDeckSF",DataTypes.StringType, true, null),
                new StructField("OpenPorchSF",DataTypes.StringType, true, null),
                new StructField("EnclosedPorch",DataTypes.StringType, true, null),
                new StructField("3SsnPorch",DataTypes.StringType, true, null),
                new StructField("ScreenPorch",DataTypes.StringType, true, null),
                new StructField("PoolArea",DataTypes.StringType, true, null),
                new StructField("PoolQC",DataTypes.StringType, true, null),
                new StructField("Fence",DataTypes.StringType, true, null),
                new StructField("MiscFeature",DataTypes.StringType, true, null),
                new StructField("MiscVal",DataTypes.StringType, true, null),
                new StructField("MoSold",DataTypes.StringType, true, null),
                new StructField("YrSold",DataTypes.StringType, true, null),
                new StructField("SaleType",DataTypes.StringType, true, null),
                new StructField("SaleCondition", DataTypes.StringType, true, null),
                new StructField("SalePrice",DataTypes.IntegerType,true, null)});

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

