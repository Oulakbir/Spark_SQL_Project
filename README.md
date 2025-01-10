# Spark SQL Project 
## TP 3: Spark SQL  

### Project Description  
This project focuses on using **Apache Spark SQL** for processing and analyzing industrial incidents and hospital data. It aims to showcase the power of Spark in parallel and distributed data processing to extract meaningful insights.  
---
### Implementation  

#### Exercice 1: Industrial Incident Analysis  
- Develop a Spark application to process incidents stored in a CSV file.
```csv
Id,titre,description,service,date
1,Incident 1,Problème mineur,Service A,2022-03-15
2,Incident 2,Panne système,Service B,2023-01-10
3,Incident 3,Panne réseau,Service A,2021-05-12
4,Incident 4,Erreur de connexion,Service C,2022-06-11
5,Incident 5,Problème de sécurité,Service B,2021-07-24
6,Incident 6,Maintenance imprévue,Service A,2022-02-18
7,Incident 7,Bug logiciel,Service C,2023-04-23
8,Incident 8,Erreur matérielle,Service D,2022-08-01
9,Incident 9,Incident mineur,Service A,2023-09-14
10,Incident 10,Erreur de configuration,Service B,2022-10-19
11,Incident 11,Panne de courant,Service C,2021-12-11
12,Incident 12,Problème réseau,Service A,2023-03-27
13,Incident 13,Erreur humaine,Service B,2021-04-05
14,Incident 14,Défaillance matérielle,Service C,2022-07-08
15,Incident 15,Problème de base de données,Service D,2023-01-25
16,Incident 16,Défaillance logicielle,Service A,2021-09-17
17,Incident 17,Erreur critique,Service B,2023-05-30
18,Incident 18,Problème réseau,Service C,2022-11-11
19,Incident 19,Panne matérielle,Service D,2021-02-15
20,Incident 20,Incident mineur,Service A,2023-06-19
21,Incident 21,Erreur de sécurité,Service B,2022-04-09
22,Incident 22,Problème logiciel,Service C,2021-03-22
23,Incident 23,Panne serveur,Service D,2022-09-15
24,Incident 24,Incident critique,Service A,2023-07-12
25,Incident 25,Problème de réseau,Service B,2021-10-21
26,Incident 26,Erreur mineure,Service C,2023-02-19
27,Incident 27,Panne logicielle,Service D,2022-05-05
28,Incident 28,Problème matériel,Service A,2021-12-14
29,Incident 29,Panne critique,Service B,2023-09-03
30,Incident 30,Bug système,Service C,2022-01-16
31,Incident 31,Erreur logicielle,Service D,2021-11-20
32,Incident 32,Incident réseau,Service A,2023-05-10
33,Incident 33,Erreur matérielle,Service B,2022-08-14
34,Incident 34,Panne générale,Service C,2021-06-22
35,Incident 35,Problème de connectivité,Service D,2022-12-29
36,Incident 36,Bug mineur,Service A,2023-02-14
37,Incident 37,Défaillance serveur,Service B,2021-01-19
38,Incident 38,Problème critique,Service C,2022-10-23
39,Incident 39,Panne réseau,Service D,2021-09-25
40,Incident 40,Incident matériel,Service A,2023-07-06
41,Incident 41,Erreur utilisateur,Service B,2022-03-28
42,Incident 42,Défaillance réseau,Service C,2021-07-14
43,Incident 43,Erreur critique,Service D,2022-06-04
44,Incident 44,Problème serveur,Service A,2023-04-12
45,Incident 45,Panne système,Service B,2021-05-09
46,Incident 46,Problème de sécurité,Service C,2022-11-22
47,Incident 47,Incident mineur,Service D,2021-08-17
48,Incident 48,Erreur réseau,Service A,2023-06-20
49,Incident 49,Problème critique,Service B,2022-09-13
50,Incident 50,Panne matérielle,Service C,2021-10-07
```
- **Tasks:**  
  1. Display the number of incidents per service.
```java
            System.out.println("Incident count per service:");
            Dataset<Row> incidentsByService = incidents.groupBy("service")
                    .agg(functions.count("id").alias("incident_count"));
            incidentsByService.show();
```

![image](https://github.com/user-attachments/assets/e5511345-e0f3-4fc8-a5d7-573ec90c336d)


  2. Identify the two years with the highest number of incidents.
```java
            System.out.println("Top two years with most incidents:");
            Dataset<Row> incidentsByYear = incidents
                    .withColumn("year", functions.year(incidents.col("date")))
                    .groupBy("year")
                    .agg(functions.count("id").alias("incident_count"))
                    .orderBy(functions.desc("incident_count"))
                    .limit(2);
            incidentsByYear.show();
```

![image](https://github.com/user-attachments/assets/a318108d-27aa-4411-a49c-0d272d86e9c0)
---
#### Exercice 2: Hospital Data Analysis  
- Utilize Spark SQL APIs (DataFrame and Dataset) to process hospital data stored in MySQL and CSV files.  
- **Tasks:**  
  1. Display the number of consultations per day.  
  2. Display the number of consultations per doctor in the format:  
     **NOM | PRENOM | NOMBRE DE CONSULTATION**  
  3. Display the number of unique patients assisted by each doctor.  

---
### Prerequisites  
- **Spark:** Apache Spark 3.x  
- **Database:** MySQL  
- **Languages:** Scala or Python  
- **Data Source:**  
  - CSV files (for incidents)  
  - MySQL database (for hospital data)  

---

### Data Details  

#### CSV Format for Incidents  
```
Id, titre, description, service, date
```

![Screenshot 2025-01-10 015426](https://github.com/user-attachments/assets/b3fdeb90-6edd-4706-9f6b-058f32537fe7)


#### MySQL Database: `DB_HOPITAL`  
- **Table 1:** `PATIENTS`  
  - `ID_PATIENT`, `NOM`, `PRENOM`, `DATE_NAISSANCE`  
- **Table 2:** `MEDECINS`  
  - `ID_MEDECIN`, `NOM`, `PRENOM`, `SPECIALITE`  
- **Table 3:** `CONSULTATIONS`  
  - `ID_CONSULTATION`, `ID_PATIENT`, `ID_MEDECIN`, `DATE_CONSULTATION`

![Screenshot 2025-01-10 015016](https://github.com/user-attachments/assets/9e5b291f-049e-42b8-ae95-977bf7a68442)

---

### Setup Instructions  

1. Prepare the CSV file for industrial incidents.  
2. Load the MySQL tables with relevant hospital data.  

![Screenshot 2025-01-10 015109](https://github.com/user-attachments/assets/aa799272-4206-4b31-8157-47265806fd38)

![Screenshot 2025-01-10 015143](https://github.com/user-attachments/assets/315e370b-c07e-4611-9279-3e6c71687135)

![Screenshot 2025-01-10 015208](https://github.com/user-attachments/assets/9169cdd6-0637-4a78-8569-4c416d36cc3f)

3. Code Review.

```java
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
```

---

### Results  
 
1. **Number of consultations per day:**

![image](https://github.com/user-attachments/assets/8bbcde3f-8241-4641-895d-a26e58135544)


2. **Consultations per doctor:**  

![image](https://github.com/user-attachments/assets/cd8bbd30-1040-4353-a7a7-3a2e9165f5e2)


3. **Number of unique patients assisted per doctor:**  

![image](https://github.com/user-attachments/assets/09dc2858-ce37-46a2-bf8e-f80013e0001f)


---

### Key Insights  
- Spark SQL simplifies large-scale data processing for both structured and semi-structured data.  
- Parallel and distributed processing enhances efficiency when dealing with large datasets.  

---

### Technologies Used  
- Apache Spark (SQL, DataFrame, Dataset APIs)  
- MySQL  
- CSV  

---

### Authors  
- **Mrs. Ilham OULAKBIR**  
