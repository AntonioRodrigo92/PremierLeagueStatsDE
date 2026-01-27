package Utils

import com.typesafe.config.ConfigFactory

import java.io.File

case class ConfigurationReader(path: String) {

  private val config = ConfigFactory.parseFile(new File(path)).resolve()
  private val mariadb = config.getConfig("mariadb")
  private val minio = config.getConfig("minio")
  private val spark = config.getConfig("spark")

  val mariadbHost = mariadb.getString("host")
  val mariadbPort = mariadb.getString("port")
  val mariadbDatabaseName = mariadb.getString("database_name")
  val mariadbUser = mariadb.getString("user")
  val mariadbPassword = mariadb.getString("password")
  val mariadbSrcTableName = mariadb.getString("src_table_name")

  val minioDestTableName = minio.getString("dest_table_name")

  val checkpointDir = spark.getString("checkpoint_dir")

}
