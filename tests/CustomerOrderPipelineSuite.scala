import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.Date

class CustomerOrderPipelineSuite extends AnyFunSuite {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("TestCustomerOrderPipeline")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  def customerSchema: StructType = StructType(Seq(
    StructField("CustId", StringType, nullable = true),
    StructField("Name", StringType, nullable = true),
    StructField("EmailId", StringType, nullable = true),
    StructField("Region", StringType, nullable = true)
  ))

  def orderSchema: StructType = StructType(Seq(
    StructField("OrderId", StringType, nullable = true),
    StructField("ItemName", StringType, nullable = true),
    StructField("PricePerUnit", DoubleType, nullable = true),
    StructField("Qty", IntegerType, nullable = true),
    StructField("Date", DateType, nullable = true),
    StructField("CustId", StringType, nullable = true)
  ))

  def sampleCustomerData(): DataFrame = {
    val data = Seq(
      ("C001", "John Doe", "john@example.com", "North"),
      ("C002", "Jane Smith", "jane@example.com", "South"),
      ("C003", "Bob Johnson", "bob@example.com", "East"),
      ("C004", null.asInstanceOf[String], "alice@example.com", "West"),
      ("C005", "Tom Brown", "tom@example.com", "North"),
      ("C005", "Tom Brown", "tom@example.com", "North")
    )
    spark.createDataFrame(
      spark.sparkContext.parallelize(data.map { case (a,b,c,d) => org.apache.spark.sql.Row(a,b,c,d) }),
      customerSchema
    )
  }

  def sampleOrderData(): DataFrame = {
    val data = Seq(
      ("O001", "Laptop", 1200.0, 1, Date.valueOf("2023-01-15"), "C001"),
      ("O002", "Phone", 800.0, 2, Date.valueOf("2023-01-20"), "C002"),
      ("O003", "Tablet", 500.0, 1, Date.valueOf("2023-01-25"), "C003"),
      ("O004", "Headphones", 100.0, 3, Date.valueOf("2023-01-30"), "C001"),
      ("O005", "Charger", 25.0, 5, Date.valueOf("2023-02-05"), "C002"),
      ("O006", "Case", null.asInstanceOf[java.lang.Double], 2, Date.valueOf("2023-02-10"), "C003"),
      ("O007", "Screen", 50.0, 1, Date.valueOf("2023-02-15"), "C005"),
      ("O007", "Screen", 50.0, 1, Date.valueOf("2023-02-15"), "C005")
    )
    val rdd = spark.sparkContext.parallelize(
      data.map { case (a,b,c,d,e,f) => org.apache.spark.sql.Row(a,b,c,d,e,f) }
    )
    spark.createDataFrame(rdd, orderSchema)
  }

  def validateData(df: DataFrame, requiredCols: Seq[String]): Boolean = {
    val cols = df.columns.toSet
    val missing = requiredCols.filterNot(cols.contains)
    if (missing.nonEmpty) throw new IllegalArgumentException(s"Missing required columns: ${missing.mkString(", ")}")
    true
  }

  test("validate_data: valid and invalid columns") {
    val customers = sampleCustomerData()
    assert(validateData(customers, Seq("CustId", "Name", "EmailId", "Region")) === true)

    val ex = intercept[IllegalArgumentException] {
      validateData(customers, Seq("CustId", "Name", "NonExistentColumn"))
    }
    assert(ex.getMessage.contains("Missing required columns"))
  }

  test("data cleaning logic") {
    val customers = sampleCustomerData()
    val orders = sampleOrderData()

    val cleanedCustomer = customers
      .filter(col("CustId").isNotNull && col("Name").isNotNull && col("EmailId").isNotNull && col("Region").isNotNull)
      .dropDuplicates("CustId")

    assert(cleanedCustomer.count() === 4L)

    val cleanedOrder = orders
      .filter(
        col("OrderId").isNotNull &&
        col("ItemName").isNotNull &&
        col("PricePerUnit").isNotNull &&
        col("Qty").isNotNull &&
        col("Date").isNotNull &&
        col("CustId").isNotNull
      )
      .dropDuplicates("OrderId")

    assert(cleanedOrder.count() === 5L)
  }

  test("total amount calculation") {
    val orders = sampleOrderData()
    val withTotal = orders.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))

    val o001 = withTotal.filter(col("OrderId") === "O001").select("TotalAmount").first().getDouble(0)
    assert(math.abs(o001 - 1200.0) < 1e-9)

    val o002 = withTotal.filter(col("OrderId") === "O002").select("TotalAmount").first().getDouble(0)
    assert(math.abs(o002 - 1600.0) < 1e-9)
  }

  test("customer aggregate spend per day") {
    val customers = sampleCustomerData()
    val orders = sampleOrderData()

    val cleanedCustomer = customers
      .filter(col("CustId").isNotNull && col("Name").isNotNull)
      .dropDuplicates("CustId")

    val cleanedOrder = orders
      .filter(col("OrderId").isNotNull && col("PricePerUnit").isNotNull)
      .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))

    val customerSpend = cleanedOrder
      .join(cleanedCustomer.select("CustId", "Name"), Seq("CustId"), "inner")
      .groupBy(col("Name"), col("Date"))
      .agg(sum(col("TotalAmount")).as("TotalAmount"))

    val johnRows = customerSpend.filter(col("Name") === "John Doe").collect().toSeq
    assert(johnRows.size === 2)

    val jan15 = johnRows.filter(r => r.getDate(r.fieldIndex("Date")) == Date.valueOf("2023-01-15"))
    assert(jan15.size === 1)
    assert(math.abs(jan15.head.getDouble(jan15.head.fieldIndex("TotalAmount")) - 1200.0) < 1e-9)
  }
}
