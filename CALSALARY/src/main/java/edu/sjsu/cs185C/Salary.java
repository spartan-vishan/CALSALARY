package edu.sjsu.cs185C;

import java.util.Arrays;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.desc;

public class Salary {

    //input arguements follow this order:
    //san-jose-2016.csv san-jose-2017.csv san-francisco-2017.small.csv
    public static void main(String[] args) throws Exception {
        //Input files
        String fileSJ16= args[0];
        String fileSJ17= args[1];
        String fileSF17= args[2];

        //Create a Java Spark Context.
        SparkSession spark = SparkSession
                .builder()
                .appName("CaliforniaSalary")
                .getOrCreate();

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("employeeName", DataTypes.StringType, true),
                DataTypes.createStructField("jobTitle", DataTypes.StringType, true),
                DataTypes.createStructField("basePay", DataTypes.FloatType, true),
                DataTypes.createStructField("overtimePay", DataTypes.FloatType, true),
                DataTypes.createStructField("otherPay", DataTypes.FloatType, true),
                DataTypes.createStructField("benefits", DataTypes.FloatType, true),
                DataTypes.createStructField("totalPay", DataTypes.FloatType, true),
                DataTypes.createStructField("totalPayAndBenefits", DataTypes.FloatType, true),
                DataTypes.createStructField("year", DataTypes.IntegerType, true),
                DataTypes.createStructField("notes", DataTypes.StringType, true),
                DataTypes.createStructField("agency", DataTypes.StringType, true),
                DataTypes.createStructField("status", DataTypes.StringType, true));
        StructType salarySchema = DataTypes.createStructType(fields);

        //Load input data and create 3 Dataset<Row>: salary for SJ 2016, SJ 2017 and SF 2017
        Dataset<Row> salaryDFSJ16 = spark.read().option("header", true).schema(salarySchema).csv(fileSJ16);
        Dataset<Row> salaryDFSJ17 = spark.read().option("header", true).schema(salarySchema).csv(fileSJ17);
        Dataset<Row> salaryDFSF17 = spark.read().option("header", true).schema(salarySchema).csv(fileSF17);

        //Using salaryEncoder to create typed Dataset<SalaryRecord> for above inputs
        //Encoder<SalaryRecord> salaryEncoder = Encoders.bean(SalaryRecord.class);
        Encoder<SalaryRecord> salaryEncoder = Encoders.bean(SalaryRecord.class);
        Dataset<SalaryRecord> salaryDSSJ16 = salaryDFSJ16.as(salaryEncoder);
        Dataset<SalaryRecord> salaryDSSJ17 = salaryDFSJ17.as(salaryEncoder);
        Dataset<SalaryRecord> salaryDSSF17 = salaryDFSF17.as(salaryEncoder);

        long totalRecordCount = salaryDSSJ17.select("employeeName").count();
        long totalJobTitleCount = salaryDSSJ16.select("jobTitle").distinct().count();

        //TODO: count the distinct jobTitles in 2017 San Jose data set
        System.out.println("---2017 San Jose: Total Record Count : " + totalRecordCount + ", Total JobTile Count: " + totalJobTitleCount);

        System.out.println("---2017 San Jose: 3 records with lowest totalPayAndBenefits");
        //TODO: find and print the top 3 records with lowest totalPayAndBenefits in 2017 San Jose data set
        salaryDFSF17.sort("totalPayAndBenefits").show(3);

        System.out.println("---2017 San Jose: 3 record with highest totalPayAndBenefits");
        //TODO: find and print the top 3 records with highest totalPayAndBenefits in 2017 San Jose data set
        salaryDFSF17.orderBy(desc("totalPayAndBenefits")).show(3);

        System.out.println("---2017 San Jose: record with highest overtime pay");
        //TODO: find and print the records with highest overtimePay in 2017 San Jose data set
        salaryDFSJ17.orderBy(desc("overtimePay")).show(1);

        System.out.println("---2017 San Jose: record with highest benefit pay");
        //TODO: find and print the highest benefit records in 2017 San Jose data set
        salaryDFSJ17.orderBy(desc("benefits")).show(1);

        System.out.println("---2017 San Francisco: 3 records with lowest totalPayAndBenefits");
        //TODO: find and print the lowest totalPayAndBenefit records in 2017 San Francisco data set
        salaryDFSF17.sort("totalPayAndBenefits").show(3);

        System.out.println("---2017 San Francisco 3 records with highest totalPayAndBenefits");
        //TODO: find and print the highest totalPayAndBenefit records in 2017 San Francisco data set
        salaryDFSF17.orderBy(desc("totalPayAndBenefits")).show(3);

        String jobTitlePO = "Police Officer";
        System.out.println("---2017 San Jose: highest totalPayAndBenefits for job " + jobTitlePO);
        //TODO: In San Jose 2017 data set, find the highest totalPayAndBenefits for the jobTitle "Police Officer"
        salaryDFSJ17.filter("jobTitle = 'Police Officer' ").sort(desc("totalPayAndBenefits")).show(1);

        System.out.println("---2017 San Francisco: highest totalPayAndBenefits for job " + jobTitlePO);
        //TODO: In 2017 San Francisco data set, find the highest totalPayAndBenefits for the jobTitle "Police Officer"
        salaryDFSF17.filter("jobTitle = 'Police Officer' ").sort(desc("totalPayAndBenefits")).show(1);


        System.out.println("---2017 San Jose: the job with highest average TotalPayAndBenefits");
        //TODO: print the jobTitle with best average totalPayAndBenefits in 2017 San Jose data set
        salaryDFSJ17.groupBy("jobTitle").avg("totalPayAndBenefits").toDF("jobTitle", "avgTotalPayAndBenefitsSJ17").orderBy(desc("avgTotalPayAndBenefitsSJ17")).show(1);

        System.out.println("---2017 San Jose: the 2 jobs with biggest difference between max and min TotalPayAndBenefits");
        //TODO: print the jobTitle with biggest difference between max and min totalPayAndBenefits in 2017 San Jose data set, and the difference

        Dataset<Row> maxSalary = salaryDFSJ17.groupBy("jobTitle").max("totalPayAndBenefits").toDF("jobTitle", "maxTotalPayAndBenefitsSJ17");
        Dataset<Row> minSalary = salaryDFSJ17.groupBy("jobTitle").min("totalPayAndBenefits").toDF("jobTitle", "minTotalPayAndBenefitsSJ17");
        Dataset<Row> compareMaxMin = maxSalary.join(minSalary, "jobTitle");

        //compare max/min cols
        Dataset<Row> finalDataSet = compareMaxMin.withColumn(
                "diffMaxMinTotalPayAndBenefits",
                col("maxTotalPayAndBenefitsSJ17").minus(
                        col("minTotalPayAndBenefitsSJ17")
                ));
        //sort and output
        finalDataSet.sort(desc("diffMaxMinTotalPayAndBenefits")).show(2);


        System.out.println("---2016-2017 San Jose: the 2 jobs with biggest increase of avg TotalPayAndBenefits");
        //TODO: In 2017 San Jose data set, find the jobTitle with highest average totalPayAndBenefits increase from 2016
        Dataset<Row> avgSJ16DS = salaryDFSJ16.groupBy("jobTitle").
                avg("totalPayAndBenefits").
                toDF("jobTitle", "avgTotalPayAndBenefitsSJ16");
        Dataset<Row> avgSJ17DS = salaryDFSJ17.groupBy("jobTitle").
                avg("totalPayAndBenefits").
                toDF("jobTitle", "avgTotalPayAndBenefitsSJ17");

        Dataset<Row> joinedSJ1617DS = avgSJ16DS.join(avgSJ17DS,"jobTitle");
        Dataset<Row> resultSJ1617DS = joinedSJ1617DS.withColumn("diff1617TotalPayAndBenefits", col("avgTotalPayAndBenefitsSJ17").minus(col("avgTotalPayAndBenefitsSJ16")));
        resultSJ1617DS.sort(desc("diff1617TotalPayAndBenefits")).
                show(2);

        spark.stop();

    }
}
