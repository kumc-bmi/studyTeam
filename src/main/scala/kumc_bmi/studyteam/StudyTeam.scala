package kumc_bmi.studyteam

import java.io.PrintStream
import java.sql.Connection
import java.sql.Connection
import java.sql.ResultSet
import java.util.Properties
import scala.util.{ Try, Success, Failure }

object StudyTeam {
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

    if (flag("--select")) {
      val result = DbState.reader(src).run(querySum)
      println(result)
    }

    if (flag("--dump-schema")) {
      DBExplore.dumpSchema(src, System.out)
    }

    arg("--by-name") foreach { name =>
      byName(src, name, System.out)
    }

    if (flag("--explore-roles")) {
      exploreRoles(src, System.out)
    }

    arg("--by-title") foreach  { title =>
      byTitle(src, title, System.out)
    }

    arg("--by-id") foreach  { id =>
      byId(src, id, System.out)
    }

  }

  def exploreQuery(src: Connector, out: PrintStream, sql: String, params: Option[Array[String]] = None) {
    out.println(s"query: $sql params: $params")
    DbState.reader(src).run(DB.query(sql, params) { results =>
      new RsIterator(results)
        .foreach {r => DBExplore.printRow(r, out) }
    })
  }

  def byName(src: Connector, name: String, out: PrintStream) {
    out.println(s"_studyTeamMemberInfo $name")
    exploreQuery(src, out,
      "SELECT * from _studyTeamMemberInfo where [studyTeamMember.lastName] = ?", Some(Array(name)))

    out.println(s"_studyTeamMemberInfo $name w/protocol")
    exploreQuery(src, out,
      "SELECT * from _studyTeamMemberInfo tm join KU_IRBSubmissionView irb on irb.ParentStudyOid =tm.owningEntity where [studyTeamMember.lastName] = ?", Some(Array(name)))
  }

  def exploreRoles(src: Connector, out: PrintStream) {
    out.println("_studyTeamMemberRole")
    exploreQuery(src, out,
      "SELECT TOP 20 * from _studyTeamMemberRole")

    out.println("_studyTeamMemberRole")
    exploreQuery(src, out,
      "SELECT distinct name from _studyTeamMemberRole")
  }

  def byTitle(src: Connector, title: String, out: PrintStream) {
    out.println("KU_IRBSubmissionView")
    exploreQuery(src, out,
      "SELECT * from KU_IRBSubmissionView where [Full Study Title] like ('%' + ? + '%')", Some(Array(title)))

    println(s"$title members?")
    exploreQuery(src, out,
      """
SELECT p.lastName, p.firstName
     , p.userId, p.[Employee ID]
     , p.EmailPreferred, p.BusinesPhone
     , p.accountDisabled
     , irb.ID, irb.State, irb.[Date Expiration], irb.ParentStudyOid, irb.[Full Study Title]
from _studyTeamMemberInfo tm
join KU_IRBSubmissionView irb on irb.ParentStudyOid = tm.owningEntity
join KU_PersonView p on p.OID = tm.[studyTeamMember.oid]
where [Full Study Title] like ('%' + ? + '%') and irb.State = 'Approved'
""", Some(Array(title)))
  }

  def byId(src: Connector, id: String, out: PrintStream) {
    out.println(s"Members of study with ID=$id:")
    exploreQuery(src, out,
      """
SELECT p.[Employee ID], p.userId, p.accountDisabled
     , p.lastName, p.firstName, p.EmailPreferred, p.BusinesPhone
     , irb.ID, irb.State, irb.[Date Expiration], irb.[Full Study Title]
from _studyTeamMemberInfo tm
join KU_IRBSubmissionView irb on irb.ParentStudyOid = tm.owningEntity
join KU_PersonView p on p.OID = tm.[studyTeamMember.oid]
where irb.ID = ?
""", Some(Array(id)))
  }

  class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
    def hasNext: Boolean = rs.next()
    def next(): ResultSet = rs
  }

  def querySum: DB[String] = DB.query("SELECT 1+1 as sum") { results =>
    results.next() // hmm... Try?
    results.getString("sum")
  }

  def getConfig(resource: Option[String]): Try[java.util.Properties] = {
    import java.sql.DriverManager

    val config_fn = resource.getOrElse("ecompliance.properties")
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
