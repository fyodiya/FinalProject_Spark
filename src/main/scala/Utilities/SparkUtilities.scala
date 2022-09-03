package Utilities

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtilities {

  /**
   * returns a new Spark session
   *
   * @param appName        name of our Spark instance
   * @param partitionCount by default is 5
   * @param verbose        prints info for debugging purposes
   * @param master         by default it is "local", master URL to connect to
   * @return sparkSession
   */
  def getOrCreateSpark(appName: String, partitionCount: Int = 1,
                       master: String = "local",
                       verbose: Boolean = true): SparkSession = {
    if (verbose)
      println(s"$appName with Scala version: ${util.Properties.versionNumberString}.")
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    sparkSession.conf.set("spark.sql.shuffle.partitions", partitionCount)
    if (verbose)
      println(s"Session started on Spark version ${sparkSession.version} with $partitionCount partition(s).")
    sparkSession
  }

  /**
   * creation of a temporary view
   *
   * @param spark       spark session
   * @param filePath    path to CSV file
   * @param source      CSV file used a source of data
   * @param viewName    name of our temp.view instance
   * @param header      column name in our dataframe
   * @param inferSchema Infer schema will automatically guess the data types for each field
   * @param printSchema method to display the schema of a dataframe
   * @return temporary view
   */
  def readDataWithView(spark: SparkSession,
                       filePath: String,
                       source: String = "csv",
                       viewName: String = "dfTable",
                       header: Boolean = true,
                       inferSchema: Boolean = true,
                       printSchema: Boolean = true,
                       cacheOn: Boolean = true): DataFrame = {
    val df = spark.read.format(source)
      .option("header", header.toString) //Spark wants string here since option is generic
      .option("inferSchema", inferSchema.toString) //we let Spark determine schema
      .load(filePath)
    //if you pass only whitespace or nothing to view we will not create it
    //if viewName is NOT blank
    if (viewName.nonEmpty) {
      df.createOrReplaceTempView(viewName)
      println(s"Created Temporary View for SQL queries called: $viewName")
    }
    if (printSchema)
      df.printSchema()
    if (cacheOn)
      df.cache()
    df
  }

}