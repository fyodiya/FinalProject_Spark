import Utilities.SparkUtilities
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{ RFormula, VectorAssembler}
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

  dfWithReturn.printSchema()

//  val dfForProcessing = dfWithReturn
////    .over(Window.partitionBy("ticker").orderBy("date"))
//    .withColumn("date", col("date").cast("Double"))
//    .withColumn("ticker", col("ticker").cast("Double"))
//    .withColumn("close", col("close").cast("Double"))
//    .withColumn("open", col("close").cast("Double"))
//    .withColumn("dailyReturn", col("dailyReturn").cast("Double"))

//dfForProcessing.createOrReplaceTempView("dfForProcessing")

  //takes as input a number of columns of Boolean, Double, or Vector
  //indexing strings for VectorAssembler

  val dateIndexer = new StringIndexer()
    .setInputCol("date")
    .setOutputCol("dateInd")
//  dateIndexer.fit(dfWithReturn)
//      .transform(dfWithReturn)
  val tickerInd = dateIndexer.fit(dfWithReturn).transform(dfWithReturn.select("ticker"))
  val ohe = new OneHotEncoder()
    .setInputCol("dateInd")
//    .setOutputCol("dateEnc")
  ohe.transform()

  val va = new VectorAssembler()
    .setInputCols(Array("dateInd", "open", "close", "dailyReturn"))
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
    .setNumFolds(2) //as advised by the book, may be changed to 2

//  val model = pipeline.fit(dfForProcessing)
//  val prediction = model.transform(dfForProcessing)
//prediction.show()

}
