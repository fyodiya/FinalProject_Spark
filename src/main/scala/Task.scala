import org.apache.spark.sql.functions.{avg, col, expr, lag, round, to_date}
import org.apache.spark.sql.expressions.Window

object Task extends App {

  val spark = Utilities.SparkUtilities.getOrCreateSpark("spark")

  //Load the stock

  val filePath = "src/main/scala/resources/stock_prices.csv"
  val dfWithView = Utilities.SparkUtilities.readDataWithView(spark, filePath)
//  val dfStock = spark.read
//    .format("csv")
//    .option("header", value = true)
//    .option("inferSchema", value = true)
//    .load(filePath)

//  val dfWithDate = dfStock
    val dfWithDate = dfWithView
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

  dfWithDate.cache()

  //compute the average daily return of every stock for every date

    //calculation of an average daily return goes like this:
      //daily return = close - open
    //summing up all the daily returns and dividing the sum by the number of periods (in this case - days)

  val dailyReturn = round(expr("close - open"), 2)
  val dfWithReturn = dfWithDate
    .withColumn("dailyReturn", dailyReturn)
    .orderBy("date")
    .select("date", "ticker", "dailyReturn")
//.show(10, truncate = false)

  val avgDailyReturn = round(avg(col("dailyReturn")),2).as("avgDailyReturn")
  dfWithReturn.groupBy("ticker").agg(avgDailyReturn)
  //.show(10, truncate = false)

  val dfAvgReturn = dfWithReturn
    .groupBy("date")
    .agg(avgDailyReturn.as("average_return"))
    .orderBy("date")
  dfAvgReturn.show(20, truncate = false)

  //Save the results to the file as Parquet (CSV and SQL formats are optional)

  dfAvgReturn.write
    .mode("overwrite")
    .parquet("src/resources/parquet/average_return.parquet")




  //subtract the opening price from the closing price
  //multiply the difference by the stock volume to find the total daily return
  //sum/count for the avg




//  dfStock.createOrReplaceTempView("dfWithPartitions")









  //Which stock was traded most frequently - as measured by closing price * volume - on average?


}
