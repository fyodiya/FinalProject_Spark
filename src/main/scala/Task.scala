object Task extends App {

  val spark = Utilities.SparkUtilities.getOrCreateSpark("spark")

  //load the stock

  val src = "src/main/scala/resources/stock_prices.csv"
  val df = spark.read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load(src)

  df.show(10, truncate = false)

  //compute the average daily return of every stock for every date


}
