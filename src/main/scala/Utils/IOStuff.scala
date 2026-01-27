package Utils

import java.util.Properties

object IOStuff {

  def createMariaDBConnection(host: String, port: String, dbName: String, user: String, password: String): (String, Properties) = {
    val jdbcUrl: String = s"jdbc:mariadb://$host:$port/$dbName"
    val connectionProperties: Properties = new java.util.Properties()
    connectionProperties.setProperty("user", user)
    connectionProperties.setProperty("password", password)
    connectionProperties.put("sessionInitStatement", "SET sql_mode='ANSI_QUOTES'")
    connectionProperties.setProperty("driver", "org.mariadb.jdbc.Driver")

    (jdbcUrl, connectionProperties)
  }

}
