package Utils

import com.typesafe.config.{ConfigFactory, ConfigList, ConfigValue}

import java.io.File

case class ConfigurationReader(path: String) {

  private val config = ConfigFactory.parseFile(new File(path)).resolve()
  private val mariadb = config.getConfig("mariadb")
  private val minio = config.getConfig("minio")
  private val spark = config.getConfig("spark")

  val mariadbHost: String = mariadb.getString("host")
  val mariadbPort: String = mariadb.getString("port")
  val mariadbDatabaseName: String = mariadb.getString("database_name")
  val mariadbUser: String = mariadb.getString("user")
  val mariadbPassword: String = mariadb.getString("password")
  val mariadbSrcTablesName: ConfigList = mariadb.getList("src_tables_name")
  val mariadbPredictTableName: String = mariadb.getString("predict_table_name")
  val minioDestTableName: String = minio.getString("dest_table_name")
  val checkpointDir: String = spark.getString("checkpoint_dir")

  def jsonToElements(jsonStr: ConfigValue): (Boolean, String) = {
    val conf = ConfigFactory.parseString(jsonStr.render())
    (conf.getBoolean("enabled"), conf.getString("table"))
  }

}
