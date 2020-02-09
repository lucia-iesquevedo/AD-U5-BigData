import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MadridEvents extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().
    master("local").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR");

  val madrid = spark.read.option("header", "true").option("encoding", "windows-1252").csv("datos/206974-0-agenda-eventos-culturales-100.csv")

  println("Schema: ")
  madrid.printSchema()

  println("All data:")
  madrid.show(false)

  //Filter ($("nameColumn") === $"name")
  println("Events at VALDEFUENTES: ")
  madrid.filter(col("BARRIO-INSTALACION").equalTo("VALDEFUENTES")).show()

  //Select
  println("Title and location of events at VALDEFUENTES: ")
  madrid.filter(col("BARRIO-INSTALACION").equalTo("VALDEFUENTES"))
    .select(col("TITULO"),col("NOMBRE-INSTALACION")).foreach(row => println(row(0)+ " performed at "+row(1)))

 //WithColumn
  println("\nTitle and type of events at VALDEFUENTES: ")
 madrid.filter($"BARRIO-INSTALACION".equalTo("VALDEFUENTES"))
    .withColumn("TIPO", substring_index($"TIPO","/",-1))
    .select(col("TITULO"),col("TIPO")).foreach(row => println(row(1)+ ": "+row(0)))

  //Sort
  println("\nEvents at VALDEFUENTES ordered by month: ")
  madrid.filter($"BARRIO-INSTALACION".equalTo("VALDEFUENTES"))
    .withColumn("MONTH", month($"FECHA"))
    .sort("MONTH")
    .select(col("TITULO"),col("NOMBRE-INSTALACION"), col("MONTH"))
    .show()
//

//Collect
  println("\nEvents at VALDEFUENTES ordered by date (Saved into a list): ")
  val lista = madrid.filter($"BARRIO-INSTALACION".equalTo("VALDEFUENTES"))
    .select(col("TITULO"),date_format(to_date(col("FECHA")),"dd/MM/YYYY")).collect()

  lista.foreach(println);

// Group by
  println("\nLatest event time per neighborhood: ")
  madrid.groupBy($"BARRIO-INSTALACION")
    .agg(max("HORA").as("MAXTIME"))
    .show(false)

  println("\nNeighborhood with the largest offer of events: ")
  madrid.groupBy($"BARRIO-INSTALACION")
    .agg(count("BARRIO-INSTALACION").as("count"))
    .sort(desc("count")).limit(1)
    .show(false)

  println("\nNumber of musical events per location: ")
  madrid.filter(col("TIPO").like("%Musica%"))
    .groupBy($"NOMBRE-INSTALACION")
    .count()
    .show(false)

  println("\nAverage number of musical events per location: ")
  madrid.filter(col("TIPO").like("%Musica%"))
    .groupBy($"NOMBRE-INSTALACION")
    .count()
    .select(avg($"count").as("media"))
    .show(false)

  println("\nLocations with a number of musical events higher than the average: ")
  val d = madrid.filter(col("TIPO").like("%Musica%"))
    .groupBy($"NOMBRE-INSTALACION")
    .count()
    .select(avg($"count").as("media"))
    .first()
    .getDouble(0);
  println(d);

  madrid.filter(col("TIPO").like("%Musica%"))
    .groupBy($"NOMBRE-INSTALACION").count()
    .withColumn("media",lit(d))
    .filter(col("count") > col("media"))
    .show(false)

// Crosstab
  println("\nTable of events per month/postal code: ")
madrid.withColumn("MONTH",date_format(to_date($"FECHA"),"yyyy-MM"))
  .stat.crosstab("MONTH","CODIGO-POSTAL-INSTALACION")
  .sort(asc("MONTH_CODIGO-POSTAL-INSTALACION"))
  .show(false)

}
