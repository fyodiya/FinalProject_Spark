import Utilities.SparkUtilities
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.LinearRegression

object PredictionModel extends App {

  val spark = SparkUtilities.getOrCreateSpark("Spark")

  val src = "src/scala/resources/src/stock_prices.csv"
  //Build a model either trying to predict the next day's price (regression)
  // or simple UP/DOWN/UNCHANGED? classificator.
  //You can only use information information from earlier dates.

  val df = spark.read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load(src)

  df.show()

  val rFormula = new RFormula()
    .setFormula("y ~ .")
    .setLabelCol("value")
    .setFeaturesCol("features")

  val ndf = rFormula
    .fit(df)
    .transform(df)

  ndf.show(10)

  val linReg = new LinearRegression()
    .setLabelCol("value")

  val lrModel = linReg.fit(ndf)

//  val summary = lrModel.summary
//  summary.residuals.show(25)

  //You can use other information if you wish from other sources for this predictor, the only thing what you can not do is use future data. smile
  //
  //One idea would be to find and extract some information about particular stock and columns with industry data (for example APPL would be music,computers,mobile)
  //
  //Do not expect something with high accuracy but main task is getting something going.
  //
  //Good extra feature would be for your program to read any .CSV in this particular format and try to do the task. This means your program would accept program line parameters.
  //
  //Assumptions
  //
  //No dividends adjustments are necessary, using only the closing price to determine returns
  //If a price is missing on a given date, you can compute returns from the closest available date
  //Return can be trivially computed as the % difference of two prices

}
