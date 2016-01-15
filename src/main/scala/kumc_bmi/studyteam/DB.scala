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


case class TableInfo(cat: String, schem: String, name: String, ty: String) {
  def asSQL(cols: Vector[ColumnInfo]): String = {
    cols.map(_.asSQL).mkString(s"create $ty $schem.$name ( -- catalog: $cat\n", ",\n", ")\n")
  }
}
object TableInfo {
  def discover: DB[Vector[TableInfo]] = DB(c => {
    val md = c.getMetaData();
    new RsIterator(md.getTables(null, null, "%", null)).map { rs =>
      TableInfo(
        rs.getString(1), // could fail? Try? map?
        rs.getString(2),
        rs.getString(3),
        rs.getString(4))
    }.to[Vector]
  })

  def dump(src: Connector, out: PrintStream,
    exclude: Array[String] = Array("INFORMATION_SCHEMA", "sys")) {
    catalog(src, exclude) foreach { case (table, cols) => out.println(table.asSQL(cols)) }
  }

  def catalog(src: Connector, exclude: Array[String]): Vector[(TableInfo, Vector[ColumnInfo])] = {
    def withCols(ti: TableInfo) = {
      // TOP 1 is a MS SQL ism
      val cols = DbState.reader(src).run(DB.query("SELECT TOP 1 * FROM " + ti.name) { results => Relation.domains(results) })
      (ti, cols)
    }

    val tables: Vector[TableInfo] = DbState.reader(src).run(TableInfo.discover)
    tables map withCols
  }
}


case class ColumnInfo(pos: Int, label: String,
  typeCode: Int, typeName: String, precision: Int) {
  def asSQL(): String = s"  [$label] $typeName($precision) -- pos: $pos"
}

object Relation {
  def domains(results: ResultSet): Vector[ColumnInfo] = {
    val m = results.getMetaData() // ignore exceptions. hm.
    val cols = for (col <- 1 to m.getColumnCount()) yield ColumnInfo(col,
      m.getColumnLabel(col),
      m.getColumnType(col),
      m.getColumnTypeName(col),
      m.getPrecision(col))
    cols.to[Vector]
  }

  def record(results: ResultSet): Vector[(String, String, Option[String])] = {
    domains(results).zipWithIndex.map {
      case (col, ix) => (col.label, col.typeName, Option(results.getString(ix + 1)))
    }
  }

  // TODO: proper quoting
  def asJSON(results: ResultSet): String = {
    // more to it than this?
    def quote(s: Option[String]) = s match {
      case Some(s) => "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
      case None => "null"
    }

    record(results) map { case (key, ty, s) => {
      val expr = ty match {
        case "int" => s match {
          case Some(s) => s
          case None => "null"
        }
        case "nvarchar" | "datetime" | "varbinary" | "binary" => quote(s)
        case other => s"????? $ty: $s"
      }

      s"""  "$key": $expr"""
    }} mkString("{\n", ",\n", "}\n")
  }
}

class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
  def hasNext: Boolean = rs.next()
  def next(): ResultSet = rs
}

