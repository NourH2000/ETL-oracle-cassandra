package com.etl


import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, when}
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
    val query = "(select id , ts , date_paiment , codeps , fk , num_enr , region , no_assure ,prix_ppa    from fraud where new =1 and applic_tarif = 'O' ) s"
    var df = sqlContext.read.format("jdbc").options(Map("url" -> url, "user" -> property(1), "password" -> property(2), "dbtable" -> query, "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
    df.printSchema()
    df.show

    // Transform the ts column from N/O to O/1

    df = df.withColumn("New_Ts", when(col("ts") === "N", 0)
      .when(col("ts") === "O", 1)
      .otherwise("Unknown")).drop("ts")




    // Transform the spark data frame to RDD
    val rows: RDD[Row] = df.rdd

    df.show
    /** **********************  Load  *********************** */

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1");
    rows.saveToCassandra("frauddetection", "ppa_source", SomeColumns("id", "date_paiment", "codeps", "fk", "num_enr","region", "no_assure", "prix_ppa" ,"ts"));


    /* Verification
    val testDF = spark.read.format("org.apache.spark.sql.cassandra").
      options(Map("table"->"test","keyspace"->"test")).
      load

    testDF.show()

     */

    println( "Hello World!" )
  }

}
