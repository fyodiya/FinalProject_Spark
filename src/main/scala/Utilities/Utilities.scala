package Utilities

import org.apache.spark.sql.DataFrame

object Utilities {

  /**
   *
   * @param dstPath - destination to the depository where the file will be saved
   * @param df - dataframe which needs to be saved
   */
  def saveDFtoParquet(df: DataFrame, dstPath: String):Unit = {
      df.write
      .mode("overwrite")
      .parquet("src/scala/resources/parquet/average_return.parquet")
    println("Saved the dataframe in a Parquet file!")
  }

  /**
   *
   * @param df dataframe that needs to be saved
   * @param dstPath destination to the depository where the file will be saved
   */
  def saveDFtoCSV(df: DataFrame, dstPath: String): Unit = {
    df.write
      .format("csv")
      .mode("overwrite")
      .option("path", "src/scala/resources/csv/average_returns.csv")
      .save()
    println("Saved the dataframe in a CSV file!")
  }


}
