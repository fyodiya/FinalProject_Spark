import Utilities.SparkUtilities
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.feature.{OneHotEncoder, RFormula, StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions.{expr, round}

object PredictionModel extends App {

  val spark = SparkUtilities.getOrCreateSpark("Spark")
  val src = "src/scala/resources/src/stock_prices.csv"

  //TASK: Build a model trying to predict the next day's price (regression)
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

  dfWithReturn.printSchema()

  val dateIndexer = new StringIndexer()
    .setInputCol("date")
    .setOutputCol("dateInd")

  val dfIndDate = dateIndexer.fit(dfWithReturn).transform(dfWithReturn)
  val dfForProcessing = dfIndDate

  val ohe = new OneHotEncoder()
    .setInputCol("dateInd")
    .setOutputCol("dateEnc")

  val supervised = new RFormula()
    .setFormula("close ~ . ")
  val fittedDF = supervised.fit(dfForProcessing)
  val preparedDF = fittedDF.transform(dfForProcessing)
  preparedDF.show()
  val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))

  val va = new VectorAssembler()
    .setInputCols(Array("dateInd", "open", "close", "high", "low", "dailyReturn"))
    .setOutputCol("features")

  val linReg = new LinearRegression()
    .setFeaturesCol("features")
    .setLabelCol("close") //since we try to predict the closing price

  val fittedLR = linReg.fit(train)
  fittedLR.transform(train).select("label", "prediction")

  val rForm = new RFormula()

  val params = new ParamGridBuilder()
//    .addGrid(rForm.formula, Array(
//      "close ~ . "
//    ))
    .addGrid(linReg.elasticNetParam, Array(0, 0.5, 1))
    .addGrid(linReg.regParam, Array(0.1, 0.2))
    .build()

  val pipeline = new Pipeline()
    .setStages(Array(va, linReg))

  val evaluator = new RegressionEvaluator()
    .setPredictionCol("prediction")
    .setLabelCol("close")

  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(params)
    .setNumFolds(3) //as advised by the book, may be changed to 2

  val modelLR = cv.fit(dfForProcessing)
  val predictionLR = modelLR.transform(dfForProcessing)

  predictionLR.show()

}
