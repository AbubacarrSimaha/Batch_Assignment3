import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.plans._

object Retail_Data {
  def main(args: Array[String]): Unit = {
    //Starting Spark Session
    val spark = SparkSession
      .builder()
      .appName("sparkSql")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val retailData = spark.read.format("json").option("header", "true").load("Resource/RetailData.json")
    //1.Print Schema
    retailData.printSchema()
    //show 30 records from the dataFrame
    retailData.show(30)

    //2.Reorder columns
    val reOrderedColumns = retailData.selectExpr("InvoiceNo", "InvoiceDateTime", "StockCode", "Description", "cast(UnitPrice as decimal(8,2)) ", "CustomerID", "cast(Quantity as Int)", "Country", "Date")
    reOrderedColumns.printSchema()
    reOrderedColumns.show(20)
    println("Count: " + reOrderedColumns.count())

    //3.Remove duplicate based on InvoiceNo, StockCode, CustomerID and InvoiceDateTime from the DataFrame using `dropDuplicates` and take count
    val removeDuplicates = reOrderedColumns.dropDuplicates("InvoiceNo", "StockCode", "CustomerID", "InvoiceDateTime")
    removeDuplicates.show(20)
    println("Count : " + removeDuplicates.count())
    //4. Add a new Column into existing DataFrame CostPrice = (UnitPrice * Quantity) using `withColumn`
    val addColumn = removeDuplicates.withColumn("CostPrice", col("UnitPrice") * col("Quantity"))
    addColumn.show(10)

    //5.
    // i)  Create a new DataFrame to calculate TotalCost per Invoice per day using `groupBy`, `agg`
    val calculateTotalCostPerInvoice = addColumn.groupBy("InvoiceNo", "Date").agg(sum("CostPrice")).orderBy("InvoiceNo", "Date")
    calculateTotalCostPerInvoice.show(10)
    //ii) rename new column to InvoiceTotalCost using  `withColumnRenamed`
    calculateTotalCostPerInvoice.withColumnRenamed("sum(CostPrice)", "InvoiceTotalCost").show(45)

    //6. Print max TotalCost with Date and InvoiceNo using `orderBy` and `first` or `take`
    val maxTotalCost = calculateTotalCostPerInvoice.orderBy(col("sum(CostPrice)").desc).first()
    println(maxTotalCost)
    //7. Create another dataFrame from DataFrame-4 with InvoiceNo and respective CustomerID and Country
    val df7=addColumn.select("InvoiceNo","CustomerID","Country")
       df7.show(3)
    //8. Join DataFrame 5 and DataFrame 7 based on InvoiceNo using `join` and select Date, CustomerID, InvoiceTotalCost using `select` or `drop`
    val joinDataFrames = calculateTotalCostPerInvoice.join(df7, calculateTotalCostPerInvoice("InvoiceNo") === df7("InvoiceNo"), "inner").select(col("Date"),col("CustomerID"),col("sum(CostPrice)"))
    joinDataFrames.show(100)
    //9. Replace null with "not registered" where customerId is null using `na.fill`
    val replaceNulllValuesDF=joinDataFrames.na.fill("not registered", Array("CustomerID"))
      replaceNulllValuesDF.show(100)
    //10.
    //i) Filter out "Not Registered" Customers using `filter` or `where`
   // val filterOutDF=replaceNulllValuesDF.filter(replaceNulllValuesDF("CustomerID") =!= "Not registered").groupBy("Date", "CustomerID").sum("InvoiceTotalCost").withColumnRenamed("sum(InvoiceTotalCost)", "CustomerTotalCost")
   // filterOutDF.show(5)

  }
}