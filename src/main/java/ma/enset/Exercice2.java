package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Exercice2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Hospital Data Analysis").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkSession spark = SparkSession.builder().sparkContext(sc.sc())
                    .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                    .getOrCreate();

            String url = "jdbc:mysql://localhost:3306/DB_HOSPITAL";
            String user = "root";
            String password = "";

            Dataset<Row> patients = spark.read()
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", "PATIENTS")
                    .option("user", user)
                    .option("password", password)
                    .load();

            Dataset<Row> medecins = spark.read()
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", "MEDECINS")
                    .option("user", user)
                    .option("password", password)
                    .load();

            Dataset<Row> consultations = spark.read()
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", "CONSULTATIONS")
                    .option("user", user)
                    .option("password", password)
                    .load();

            // 1. Display the number of consultations per day
            Dataset<Row> consultationsPerDay = consultations.groupBy("DATE_CONSULTATION")
                    .agg(functions.count("ID").alias("consultation_count"));
            consultationsPerDay.show();

            // 2. Display the number of consultations per doctor
            Dataset<Row> consultationsPerDoctor = consultations
                    .join(medecins, consultations.col("ID_MEDECIN").equalTo(medecins.col("ID")))
                    .groupBy(medecins.col("NOM"), medecins.col("PRENOM"))
                    .agg(functions.count(consultations.col("ID")).alias("consultation_count")); // Specify the consultations table here

            consultationsPerDoctor.show();

            // 3. Display the number of patients each doctor assisted
            Dataset<Row> patientsPerDoctor = consultations.join(medecins, consultations.col("ID_MEDECIN").equalTo(medecins.col("ID")))
                    .join(patients, consultations.col("ID_PATIENT").equalTo(patients.col("ID")))
                    .groupBy(medecins.col("NOM"), medecins.col("PRENOM"))
                    .agg(functions.countDistinct(patients.col("ID")).alias("patient_count"));
            patientsPerDoctor.show();

            sc.stop();
        }
    }
}


