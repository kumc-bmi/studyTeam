package kumc_bmi.studyteam

import java.sql.Connection
import java.sql.Connection
import java.util.Properties
import scala.util.{ Try, Success, Failure }

object StudyTeam {
  def main(argv: Array[String]) {
    println("hello from scala!")

    val config = getConfig(argv)
    val src = new DBConfig(config, "src")

    val result = DbState.reader(src).run(querySum)
    println(result)

    DBExplore.dumpSchema(src)
  }

  def querySum: DB[String] = DB.query("SELECT 1+1 as sum") { results =>
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
