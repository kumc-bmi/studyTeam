package kumc_bmi.studyteam

import java.io.PrintStream
import java.sql.Connection
import java.sql.ResultSet
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

object DB {
  def query[T](q: String, params: Option[Array[String]] = None): (ResultSet => T) => DB[T] = { f =>
    DB(conn => f(params match {
      case Some(ps) => {
        val s = conn.prepareStatement(q)
        for ((value, ix) <- ps zipWithIndex) {
          s.setString(ix + 1, value)
        }
        s.executeQuery()
      }
      case None => conn.createStatement().executeQuery(q)
    }))
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


object DBExplore {
  // TODO: pass output stream in as param
  def dumpSchema(src: Connector, out: PrintStream) {
    val tables = DbState.reader(src).run(tableInfo());
    tables.foreach { i =>
      if (! Array("INFORMATION_SCHEMA", "sys").contains(i.schem)) {
        out.println(s"create ${i.ty} ${i.schem}.${i.name} ( -- catalog: ${i.cat}")

        try {
          val cols = DbState.reader(src).run(exploreTable(i.name))
          cols.foreach(c => out.println(s"  ${c.label} ${c.typeName}(${c.precision}) -- ${c.pos}"))
        } catch {
          case e :Exception => out.println(" -- $e ")
        }

        out.println(")")
      }
    }
  }

  /**
   * assume name is SQL-quoting-safe
   */
  case class ColumnInfo(pos: Int, label: String,
    typeCode: Int, typeName: String, precision: Int)

  def exploreTable(name: String): DB[IndexedSeq[ColumnInfo]] = {
    // TOP 1 is a MS SQL ism
    DB.query("SELECT TOP 1 * FROM " + name) { results =>
      val m = results.getMetaData() // ignore exceptions. hm.
      for (col <- 1 to m.getColumnCount())
        yield ColumnInfo(col,
        m.getColumnLabel(col),
        m.getColumnType(col),
        m.getColumnTypeName(col),
        m.getPrecision(col))

    }
  }

  case class TableInfo(cat: String, schem: String, name: String, ty: String)

  def tableInfo(): DB[Vector[TableInfo]] = DB(c => {
    import scala.collection.immutable.VectorBuilder

    val md = c.getMetaData();
    val rs = md.getTables(null, null, "%", null); //Array("TABLE")
    val info = new VectorBuilder[TableInfo]()
    while (rs.next()) {
      info += TableInfo(
        rs.getString(1), // could fail? Try? map?
        rs.getString(2),
        rs.getString(3),
        rs.getString(4))
    }
    info.result()
  })

  def printRow(resultSet: ResultSet, on: PrintStream) {
    val rsmd = resultSet.getMetaData();
    val colQty = rsmd.getColumnCount();
    while (resultSet.next()) {
      on.println("{")
      for (i <- 1 to colQty) {
        if (resultSet.getObject(i) != null) {
          if (i > 1) on.println(",");
          val name = rsmd.getColumnName(i);
          val value = resultSet.getString(i);
          on.print(s"  ${name}: ${value}")
        }
      }
      on.println("}");
    }
  }
}
