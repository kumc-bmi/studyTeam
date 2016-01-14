package kumc_bmi.studyteam

import java.sql.Connection
import java.sql.Connection
import java.sql.ResultSet
import java.util.Properties
import scala.util.{ Try, Success, Failure }

object StudyTeam {
  def main(argv: Array[String]) {
    println("hello from scala!")

    val config = getConfig(argv)
    val src = new DBConfig(config, "src")

    //DBExplore.dumpSchema(src)
    val result = DbState.reader(src).run(querySum)
    println(result)


    println("_studyTeamMemberInfo Connolly")
    DbState.reader(src).run(
      DB.query("SELECT TOP 20 * from _studyTeamMemberInfo where [studyTeamMember.lastName] = 'Connolly'") {
        results => new RsIterator(results)
          .map(DBExplore.printRow).toList
      })

    println("_studyTeamMemberInfo Connolly w/protocol")
    DbState.reader(src).run(
      DB.query("SELECT TOP 20 * from _studyTeamMemberInfo tm join KU_IRBSubmissionView irb on irb.ParentStudyOid =tm.owningEntity where [studyTeamMember.lastName] = 'Connolly'") {
        results => new RsIterator(results)
          .map(DBExplore.printRow).toList
      })

    println("_studyTeamMemberRole")
    DbState.reader(src).run(
      DB.query("SELECT TOP 20 * from _studyTeamMemberRole") {
        results => new RsIterator(results)
          .map(DBExplore.printRow).toList
      })

    println("_studyTeamMemberRole")
    DbState.reader(src).run(
      DB.query("SELECT distinct name from _studyTeamMemberRole") {
        results => new RsIterator(results)
          .map(DBExplore.printRow).toList
      })

    println("KU_IRBSubmissionView")
    DbState.reader(src).run(
      DB.query("SELECT TOP 20 * from KU_IRBSubmissionView where [Full Study Title] like '%Healthcare Enterprise Repository%'") {
        results => new RsIterator(results)
          .map(DBExplore.printRow).toList
      })

    println("HERON members?")
    DbState.reader(src).run(
      DB.query("SELECT p.lastName, p.firstName, p.userId, p.[Employee ID], p.EmailPreferred, p.BusinesPhone, p.accountDisabled, irb.ID, irb.State, irb.[Date Expiration], irb.ParentStudyOid, irb.[Full Study Title] from _studyTeamMemberInfo tm join KU_IRBSubmissionView irb on irb.ParentStudyOid =tm.owningEntity join KU_PersonView p on p.OID = tm.[studyTeamMember.oid] where [Full Study Title] like '%Healthcare Enterprise Repository for Ontological Narration%' and irb.State = 'Approved'") {
        results => new RsIterator(results)
          .map(DBExplore.printRow).toList
      })

  }

  class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
    def hasNext: Boolean = rs.next()
    def next(): ResultSet = rs
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
