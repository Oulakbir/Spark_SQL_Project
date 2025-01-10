package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class Exercice1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Incident Analysis")
                .setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkSession spark = SparkSession
                    .builder()
                    .sparkContext(sc.sc())
                    .getOrCreate();

            Dataset<Row> incidents = spark.read()
                    .option("header", true)
                    .option("inferSchema", true)
                    .csv("incidents.csv");


            // Question 1
            System.out.println("Incident count per service:");
            Dataset<Row> incidentsByService = incidents.groupBy("service")
                    .agg(functions.count("id").alias("incident_count"));
            incidentsByService.show();

            // Question 2
            System.out.println("Top two years with most incidents:");
            Dataset<Row> incidentsByYear = incidents
                    .withColumn("year", functions.year(incidents.col("date")))
                    .groupBy("year")
                    .agg(functions.count("id").alias("incident_count"))
                    .orderBy(functions.desc("incident_count"))
                    .limit(2);
            incidentsByYear.show();

            sc.stop();
        }
    }
}