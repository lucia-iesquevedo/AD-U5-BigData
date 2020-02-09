import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EstudiantesProfesores extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().
    master("local").getOrCreate()
  import spark.implicits._


  spark.sparkContext.setLogLevel("ERROR");

  val madridJson = spark.read.json("datos/students.json")

  madridJson.printSchema()
  madridJson.show(false)

  // Explode
  println("Mark of the last call of each subject")
  madridJson.select(col("*"),explode($"subjects").as("subject")).drop("subjects")
    .select(col("fullname"), col("subject.name"),element_at(reverse(array_sort($"subject.calls")),1).as("call"))
    .withColumn("mark",col("call.mark.$numberInt").cast("Int"))
    .show(false)


  println("\nNumber of students that passed a subject per call (Table of subjects/call)")
  madridJson.select(col("*"),explode($"subjects").as("subject")).drop("subjects")
    .withColumn("calls",$"subject.calls").withColumn("nombreAsig",$"subject.name")
    .select(col("*"),element_at(reverse(array_sort($"calls")),1).as("notas"))
    .withColumn("con",col("notas.call.$numberInt").cast("Int"))
    .withColumn("nota",col("notas.mark.$numberInt").cast("Int"))
    .filter(col("nota").geq(5))
    .stat.crosstab("nombreAsig","con").show(false)


  println("\nStudents that passed a subject at the first call")
  madridJson.select(col("*"),explode($"subjects").as("subject")).drop("subjects")
    .select(col("*"),explode($"subject.calls").as("call"))
    .withColumn("con",col("call.call.$numberInt").cast("Int"))
    .withColumn("nota",col("call.mark.$numberInt").cast("Int"))
    .filter(col("con").equalTo(1) && col("nota").geq(5))
    .show(false)




}
