import Utilities.SparkUtilities
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{RFormula, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, expr, round, to_date}

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

  val dailyReturn = round(expr("close - open"), 2)
  val dfWithReturn = df
    .withColumn("dailyReturn", dailyReturn)
    .orderBy("date")
//    .select("date", "ticker", "dailyReturn")

//  val dfForProcessing = dfWithReturn
////    .over(Window.partitionBy("ticker").orderBy("date"))
//    .withColumn("date", col("date").cast("string"))
//    .withColumn("ticker", col("ticker").cast("string"))
//    .withColumn("close", col("close").cast("string"))
//    .withColumn("open", col("close").cast("string"))
//    .withColumn("dailyReturn", col("dailyReturn").cast("string"))
//
//dfForProcessing.createOrReplaceTempView("dfForProcessing")

  val va = new VectorAssembler()
    .setInputCols(Array("date", "open", "close", "ticker", "dailyReturn"))
    .setOutputCol("features")

  val linReg = new LinearRegression()
    .setFeaturesCol("features")
    .setLabelCol("close") //since we try to predict the closing price

  val pipeline = new Pipeline()
    .setStages(Array(va, linReg))

  val params = new ParamGridBuilder()
    .addGrid(linReg.regParam, Array(0, 0.5, 1))
    .build()

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setPredictionCol("prediction")
    .setLabelCol("close")

  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(params)
    .setNumFolds(3) //as advised by the book, may be changed to 2

  val model = pipeline.fit(dfWithReturn)



}
