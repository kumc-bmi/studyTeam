package kumc_bmi.studyteam

import java.util.Properties
import java.sql.Connection
import java.sql.Connection
import java.sql.ResultSet

object StudyTeam {
  def main(argv: Array[String]) {
    println("hello from scala!")

    val config = getConfig(argv)
    val src = new DBConfig(config, "src")

    val result = DbState.reader(src).run(querySum)
    println(result)
  }

  def query[T](q: String): (ResultSet => T) => DB[T] = { f =>
    DB(conn => f(conn.createStatement().executeQuery(q)))
  }

  def querySum: DB[String] = StudyTeam.query("SELECT 1+1 as sum") { results =>
    results.next() // hmm... Try?
    results.getString("sum")
  }

  def getConfig(argv: Array[String]): java.util.Properties = {
    import java.sql.DriverManager

    // KLUDGE: can return null
    val config_fn = if (argv.length > 0) { argv(0) } else { "ecompliance.properties" }
    val config = this.getClass().getResourceAsStream(config_fn)
    val p = new Properties()
    // KLUDGE: not handling exceptions here
    p.load(config)

    val verbose = "true" == p.getProperty("verbose")
    if (verbose) {
      DriverManager.setLogStream(System.err)
    }

    p
  }
}

class DBConfig(p: java.util.Properties, name: String) extends Connector {
  import java.sql.DriverManager

  override def connect(): Connection = connect(new Properties)
  override def connect(connectionProperties: Properties): java.sql.Connection = {
    val driver = p.getProperty(name + ".driver")
    val url = p.getProperty(name + ".url")
    // sloppy in-place update
    connectionProperties.put("user", p.getProperty(name + ".username"))
    connectionProperties.put("password", p.getProperty(name + ".password"))
    Class.forName(driver)
    // throws an exception. hmm.
    DriverManager.getConnection(url, connectionProperties)
  }
}
