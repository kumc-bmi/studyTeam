package kumc_bmi.studyteam

import java.io.PrintStream
import java.sql.Connection
import java.sql.Connection
import java.sql.ResultSet
import java.util.Properties
import scala.util.{ Try, Success, Failure }

/**
  * Provides HTTP access to study teams from e-compliance DB.
  *
  * @see [[StudyTeam.main]] for command-line usage.
  */
object StudyTeam {
  /**
    * Usage:
    *   `studyteam [--config ''RESOURCE''] --serve`
    *
    * Database access properties (src.driver, src.url, src.username,
    * src.password) are taken from the config properties resource,
    * which may be given as a CLI option.
    *
    * Port for HTTP server may be given in an http.port property.
    *
    * Exploratory options:
    *   - `--select`       run a select query to test the DB connection.
    *   - `--dump-schema`  enumerate tables and columns in the DB.
    *   - `--by-name ''WHO''`  see [[byName]]
    *   - `--by-title ''T''`   see [[byTitle]]
    *   - `--by-id`        see [[byId]]    }
    *
    * @see [[defaultConfig]]
    * @see [[defaultPort]]
    */
  def main(argv: Array[String]) {
    def flag(sentinel: String): Boolean = argv.contains(sentinel);
    def arg(sentinel: String): Option[String] =
      (argv.indexOf(sentinel), argv.length) match {
        case (i, l) if i >= 0 && i + 1 < l => Some(argv(i + 1))
        case (i, l) if i >=0 => {
          System.err.println(s"$sentinel requires value argument")
          None
        }
        case _ => None
      }

    val config = getConfig(arg("--config")) match {
      case Success(c) => c
      case Failure(f) => {
        System.err.println(f)
        System.exit(1)
        return
      }
    }
    val src = new DBConfig(config, "src")

    // leave encrypt/decrypt undocumnted for now.
    arg("--encrypt") foreach { clearText =>
      println(new SymmetricKey("password12345678").encrypt(clearText))
    }

    arg("--decrypt") foreach { cipherText =>
      println(new SymmetricKey("password12345678").decrypt(cipherText))
    }

    if (flag("--select")) {
      val result = DbState.reader(src).run(querySum)
      println(result)
    }

    if (flag("--dump-schema")) {
      TableInfo.dump(src, System.out)
    }

    arg("--by-name") foreach { name =>
      byName(src, name, System.out)
    }

    // undocumented; turns out to not be useful.
    if (flag("--explore-roles")) {
      exploreRoles(src, System.out)
    }

    arg("--by-title") foreach  { title =>
      byTitle(src, title, System.out)
    }

    arg("--by-id") foreach  { id =>
      byId(src, id, System.out)
    }

    if (flag("--serve")) {
      // Silently ignore syntax errors in http.port property
      val port = Try(config.getProperty("http.port").toInt)
        .getOrElse(defaultPort);
      new StudyServer(src, port).start()
    }
  }

  /** HTTP server port */
  val defaultPort = 8080;

  private def exploreQuery(src: Connector, out: PrintStream, sql: String, params: Option[Array[String]] = None) {
    out.println(s"query: $sql params: $params")

    def printResult(results: ResultSet) {
      new RsIterator(results) map {r => out.println(Relation.asJSON(r)) }}

    val exploreQuery = DB.query(sql, params) map printResult
    DbState.reader(src).run(exploreQuery)
  }

  /** Show `_studyTeamMemberInfo` records with a given last name.
    *
    * Also show such records joined with `KU_IRBSubmissionView`.
    */
  def byName(src: Connector, name: String, out: PrintStream) {
    out.println(s"_studyTeamMemberInfo $name")
    exploreQuery(src, out,
      "SELECT * from _studyTeamMemberInfo where [studyTeamMember.lastName] = ?", Some(Array(name)))

    out.println(s"_studyTeamMemberInfo $name w/protocol")
    exploreQuery(src, out,
      "SELECT * from _studyTeamMemberInfo tm join KU_IRBSubmissionView irb on irb.ParentStudyOid =tm.owningEntity where [studyTeamMember.lastName] = ?", Some(Array(name)))
  }

  private def exploreRoles(src: Connector, out: PrintStream) {
    out.println("_studyTeamMemberRole")
    exploreQuery(src, out,
      "SELECT TOP 20 * from _studyTeamMemberRole")

    out.println("_studyTeamMemberRole")
    exploreQuery(src, out,
      "SELECT distinct name from _studyTeamMemberRole")
  }

  /** Show `KU_IRBSubmissionView` records whose title contains some text.
    *
    * Also show associated `_studyTeamMemberInfo` details.
    */
  def byTitle(src: Connector, title: String, out: PrintStream) {
    out.println("KU_IRBSubmissionView")
    exploreQuery(src, out,
      "SELECT * from KU_IRBSubmissionView where [Full Study Title] like ('%' + ? + '%')", Some(Array(title)))

    println(s"$title members?")
    exploreQuery(src, out,
      """
SELECT p.lastName, p.firstName
     , p.userId, p.ID as [Employee ID]
     , p.EmailPreferred, p.BusinesPhone
     , p.accountDisabled
     , irb.ID, irb.State, irb.[Date Expiration], irb.ParentStudyOid, irb.[Full Study Title]
from _studyTeamMemberInfo tm
join KU_IRBSubmissionView irb on irb.ParentStudyOid = tm.owningEntity
join KU_PersonView p on p.OID = tm.[studyTeamMember.oid]
where [Full Study Title] like ('%' + ? + '%')
""", Some(Array(title)))
  }

  /** Show members of study with a given ID.
    *
    * @see [[team]]
    */
  def byId(src: Connector, id: String, out: PrintStream) {
    out.println(s"Members of study with ID=$id:")
    out.println(team(src, id))
  }

  /** Return membership info of a study in JSON format.
    *
    * Returns a JSON array of objects with the following keys:
    *  - `Employee ID`, `p.userId` - strings
    *  - `accountDisabled` - int
    *  - `lastName`, `firstName` - strings
    *  - `EmailPreferred`, `BusinesPhone` - strings (or null)
    *  - `ID` - string - ID of IRB study
    *  - `State` - string
    *  - `Date Expiration` - string in ''YYYY-MM-DD HH:MM:SS.F'' format
    *  - `Full Study Title` - string
    */
  def team(src: Connector, studyId: String): String = {
    val idQ = """
SELECT p.ID as [Employee ID], p.userId, p.accountDisabled
     , p.lastName, p.firstName, p.EmailPreferred, p.BusinesPhone
     , irb.ID, irb.State, irb.[Date Expiration], irb.[Full Study Title]
from _studyTeamMemberInfo tm
join KU_IRBSubmissionView irb on irb.ParentStudyOid = tm.owningEntity
join KU_PersonView p on p.OID = tm.[studyTeamMember.oid]
where irb.ID = ?
"""
    DbState.reader(src).run(
      DB.query(idQ, Some(Array(studyId))) map { results =>
        (new RsIterator(results) map Relation.asJSON) mkString("[\n", ",\n", "]\n")
      })
  }

  /** Return a simple 1+1 calculation query.
    *
    * @see [[DbState]]
    */
  def querySum: DB[String] = DB.query("SELECT 1+1 as sum") map { results =>
    results.next() // hmm... Try?
    results.getString("sum")
  }

  /** Default resource name for DB, HTTP config.
    */
  val defaultConfig = "ecompliance.properties";

  /** Try to get config properties.
    *
    * @param resource optional name of resource
    * @see defaultConfig
    */
  def getConfig(resource: Option[String]): Try[java.util.Properties] = {
    import java.sql.DriverManager

    val config_fn = resource.getOrElse(defaultConfig)
    val klass = this.getClass()
    Option(klass.getResourceAsStream(config_fn)) match {
      case Some(stream) => {
        val p = new Properties()
        Try(p.load(stream)) map { ok =>
          if ("true" == p.getProperty("verbose")) {
            DriverManager.setLogStream(System.err)
          }
          p
        }
      }
      case None => Failure(new RuntimeException(s"$klass.getResourceAsStream($config_fn) returned null"))
    }
  }
}

/** HTTP service to look up IRB study team info by study id.
  *
  * @param port server listening port
  * @param src [[Connector]] to e-compliance DB.
  */
class StudyServer(src: Connector, port: Int) extends SimpleHttpServerBase(port=port) {
  import com.sun.net.httpserver.HttpExchange

  val pattern = "id=(.*)".r

  /** Handle requests for study team info.
    *
    * For GET requests with a query string in the format ?id=XXX,
    * consult the src DB and respond with study team info in
    * JSON format.
    *
    * Other requests result in an HTTP 404 reponse.
    *
    * @see [[StudyTeam.team]]
    */
  def handle(exchange: HttpExchange) {
    exchange.getRequestURI.getQuery match {
      case pattern(id) => {
        System.err.println(s"GET id=$id")
        respond(exchange, 200, StudyTeam.team(src, id))
      }
      case _ => respond(exchange, 404)
    }
  }
}

/** Encrypt, decrypt data w.r.t. a shared secret.
  *
  * The idea is: rather than storing the database password
  * in the properties resource in clear-text, encrypt it
  * and share the secret with authorized clients. Then neither
  * the client config nor the server config alone is enough
  * to access the DB.
  *
  * It complicates integration testing, though, so I have not yet finished
  * implementing this idea.
  *
  * Navigating the java crypto API is fraught with peril. Most
  * parameter combinations don't work, and of those that do, most are
  * insecure.  ECB is subject to replay attack, but getting CBC
  * working seems to take more time than is justifyable today.
  */
class SymmetricKey(secret: String) {
  import javax.crypto.{Cipher}
  import javax.crypto.spec.{SecretKeySpec}

  val algorithmName = "AES";  // or DES
  val secretKey = new SecretKeySpec(secret.getBytes("UTF-8"), algorithmName)
  val cipher = Cipher.getInstance(algorithmName + "/ECB/PKCS5Padding")

  def encrypt(clearText: String): String = {
    cipher.init(Cipher.ENCRYPT_MODE, secretKey)
    cipher.doFinal(clearText.getBytes("UTF8"))
      .map("%02X" format _).mkString
  }

  def decrypt(cipherHex: String): String = {
    val cipherBytes = cipherHex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)

    cipher.init(Cipher.DECRYPT_MODE, secretKey)
    new String(cipher.doFinal(cipherBytes), "UTF8")
  }
}
