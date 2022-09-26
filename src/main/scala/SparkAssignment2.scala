import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.plans._

object SparkAssignment2 {
  def main(args: Array[String]): Unit = {
    //Starting Spark Session
    val spark = SparkSession
      .builder()
      .appName("sparkSql")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //1. Create DataFrame from CSV
    val sparkDF = spark.read.format("csv").option("header", "true").load("Resource/spark_assignment.csv")
    //Print Schema
    sparkDF.printSchema()
    sparkDF.show(5)
    //2. Create a new column with hostname derived from "url" column

    //3. Filter "job_title" by any manager
    val sparkDF3 = sparkDF.filter(sparkDF("job_title").contains("Manager"))
    sparkDF3.show(false)

    //4. Highest yearly salary of each gender
    val sparkDF4 = sparkDF.withColumn("salary",
      expr("substring(salary, 2, length(salary))"))
      .withColumn("newSalary",
        when(col("payments_frequency") === "Often", col("salary") * 4)
          .when(col("payments_frequency") === "Once", col("salary") * 1)
          .when(col("payments_frequency") === "Monthly", col("salary") * 12)
          .when(col("payments_frequency") === "Seldom", col("salary") * 2)
          .when(col("payments_frequency") === "Never", col("salary") * 0)
          .when(col("payments_frequency") === "Daily", col("salary") * 365)
          .when(col("payments_frequency") === "Yearly", col("salary") * 1)
          .when(col("payments_frequency") === "Weekly", col("salary") * 52)
          .otherwise("Unknown")
          .cast("Double"))
    sparkDF4.groupBy("gender").max("newSalary").show(false)

  }
}