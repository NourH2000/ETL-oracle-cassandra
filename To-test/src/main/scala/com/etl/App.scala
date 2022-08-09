package com.etl


import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.io.Source
/**
 * @author ${user.name}
 */
object App {
  

  def main(args : Array[String]) {
    // spark session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ETL with spark ( cassandra <=> oracle)")
      .config("spark.some" +
        ".config.option", "some-value")
      .getOrCreate()
    // property file
    val property = ListBuffer[String]()
    val filename = "src/property/property.txt"
    for (line <- Source.fromFile(filename).getLines) {
      property += line
    }

    /** **********************  Extract  *********************** */

    val sqlContext = spark.sqlContext
    // property : (1)  : user , (2) : pwd , (3) : host , (4): ass
    val url = "jdbc:oracle:thin:" + property(1) + "/" + property(2) + "@//" + property(3) + "/" + property(4)
    val query = "(select  sexe as gender , ts , affection , age , date_paiement , codeps , fk , num_enr , quantite_med  , centre as region , no_assure   , prix_ppa from cnasass.test_data45   ) s"
    var df = sqlContext.read.format("jdbc").options(Map("url" -> url, "user" -> property(1), "password" -> property(2), "dbtable" -> query, "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
    df.printSchema()

    /** **********************  tranform  *********************** */

    // ajouter id :
    df = df.withColumn("id"
      , monotonically_increasing_id)


    // Transform the gender column from F/M to O/1 :

    df = df.withColumn("New_GENDER", when(col("GENDER") === "M", 0)
      .when(col("GENDER") === "F", 1)
      .otherwise("Unknown")).drop("Gender")
    df = df.withColumn("New_GENDER",col("New_GENDER").cast("int"))


    //Transform the ts column from N/O to O/1

    df = df.withColumn("New_Ts", when(col("ts") === "N", 0)
      .when(col("ts") === "O", 1)
      .otherwise("Unknown")).drop("ts")
    df = df.withColumn("New_Ts",col("New_Ts").cast("int"))

    /* Transfrom type of columns :
    df = df.withColumn("AFFECTION",col("AFFECTION").cast("string"))
    df = df.withColumn("AGE",col("AGE").cast("int"))
    df = df.withColumn("DATE_PAIEMENT",col("DATE_PAIEMENT").cast("DateType"))
    df = df.withColumn("FK",col("FK").cast("string"))
    df = df.withColumn("NUM_ENR",col("NUM_ENR").cast("double"))
    df = df.withColumn("QUANTITE_MED",col("QUANTITE_MED").cast("double"))
    df = df.withColumn("REGION",col("REGION").cast("int"))
    df = df.withColumn("NO_ASSURE",col("NO_ASSURE").cast("double"))
    df = df.withColumn("PRIX_PPA",col("PRIX_PPA").cast("double"))

*/

    // Transform the spark data frame to RDD :

    val rows: RDD[Row] = df.rdd
    df.printSchema()
    df.show()
    /** **********************  Load  *********************** */

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1");
    rows.saveToCassandra("cnas", "cnas", SomeColumns("affection", "age", "date_paiment", "codeps", "fk", "num_enr", "quantite_med", "region", "no_assure", "prix_ppa","id", "gender","ts"));

    /* Verification
    val testDF = spark.read.format("org.apache.spark.sql.cassandra").
      options(Map("table"->"test","keyspace"->"test")).
      load

    testDF.show()

     */

    println( "Hello World!" )
  }

}
