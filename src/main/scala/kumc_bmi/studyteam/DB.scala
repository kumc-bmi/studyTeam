package kumc_bmi.studyteam

import java.sql.Connection
import java.sql.SQLException
import java.util.Properties

/**
 * (cribbed from functionaljava)
 */
case class DB[A](g: Connection => A) {
  def apply(c: Connection): A = {
    g(c)
  }

  def unit(a: A) = DB(c => a)

  def map[B](f: A => B): DB[B] = {
    DB(s => f(g(s)))
  }

  def flatMap[B](f: A => DB[B]): DB[B] = {
    DB(s => f(g(s))(s))
  }

  def liftM[B](f: A => B) : DB[A] => DB[B] = { dba =>
    for {
      a <- dba
    } yield f(a)
  }
}

abstract class Connector {
  def connect(): Connection
  def connect(connectionProperties: Properties): Connection = connect()
}

class DbState(pc: Connector, terminal: DB[Unit]) {

  /**
   * Runs the given database action as a single transaction.
   */
  def run[A](dba: DB[A]): A = {
    val c = pc.connect()
    c.setAutoCommit(false)
    try {
      val a = dba(c)
      terminal(c)
      a
    } catch {
      case e: SQLException => {
              c.rollback()
              throw e
      }
    }
    finally {
      c.close()
    }
  }
}
object DbState {
  def driverManager(url: String): Connector = {
    import java.sql.DriverManager

    new Connector {
      override def connect() : Connection = {
        DriverManager.getConnection(url)
      }
    };
  }

  def reader(pc: Connector) = new DbState(pc, rollback)
  def reader(url: String) = new DbState(driverManager(url), rollback)


  def writer(url: String) = new DbState(driverManager(url), commit)
  def writer(pc: Connector) = new DbState(pc, commit)

  private val rollback = DB( c => c.rollback())

  private val commit = DB(c => c.commit())
}
