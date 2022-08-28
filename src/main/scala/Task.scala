import org.apache.spark.sql.functions.{avg, col, expr, round, to_date}

object Task extends App {

  val spark = Utilities.SparkUtilities.getOrCreateSpark("spark")


  //TASK: load the stock!

  val filePath = "src/scala/resources/src/stock_prices.csv"
  val dfWithView = Utilities.SparkUtilities.readDataWithView(spark, filePath)

  val dfWithDate = dfWithView
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))


  //TASK: Compute the average daily return of every stock for every date!

    //calculation of an average daily return goes like this:
    //daily return = close - open
    //then summing up all the daily returns and dividing the sum by the number of periods (in this case - done by Agg.)

  val dailyReturn = round(expr("close - open"), 2)
  val dfWithReturn = dfWithDate
    .withColumn("dailyReturn", dailyReturn)
    .orderBy("date")
    .select("date", "ticker", "dailyReturn")
//.show(10, truncate = false)

  val avgDailyReturn = round(avg(col("dailyReturn")),2).as("avgDailyReturn")
  val dfWithAvgReturn = dfWithReturn
    .groupBy("date").agg(avgDailyReturn.as("avgDailyReturn")).orderBy("date")


  //TASK: print the results on screen!
  //your output should have the columns:
  //date average_return yyyy-MM-dd return of all stocks on that date

  println("Average daily returns, by date:")
  dfWithAvgReturn.show()
//  dfWithDate.orderBy("date")
//    .select("date", "ticker", "average_return").show()


  //TASK: Save the results to the file as Parquet (CSV and SQL formats are optional)!
  //TODO as a method
  dfWithAvgReturn.write
    .mode("overwrite")
    .parquet("src/scala/resources/parquet/average_return.parquet")

  dfWithAvgReturn.write.format("csv")
    .mode("overwrite")
    .option("path", "src/resources/csv/average_returns.csv")
    .save()


  //TASK: Find which stock was traded most frequently
  //as measured by closing price * volume - on average


}
