package Utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object SparkUtils {

  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
//      .config("spark.master", "local")
      .appName("PremierLeagueStatsDE")
      .getOrCreate()
  }

  def stopSparkSession(implicit spark: SparkSession, sc: SparkContext): Unit = {
    sc.stop()
    spark.stop()
  }

  def deleteCheckpointDirectory(checkpointPath: String)(implicit spark: SparkSession): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fsPath = new Path(checkpointPath)
    val fs = FileSystem.get(fsPath.toUri, conf)

    if (fs.exists(fsPath)) {
      fs.delete(fsPath, true)
    }
  }

}
