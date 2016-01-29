package kumc_bmi.studyteam

import java.io.PrintStream
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.util.Properties

/** The DB monad represents composable database actions.
  *
  * ack: cribbed from `fj.control.db.DB` in
  * [[http://www.functionaljava.org/ functionaljava]]
  *
  * @see [[DbState]]
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

  def bind[B](f: A => DB[B]): DB[B] = {
    DB(c => f(g(c))(c))
  }
}

object DB {
  /** Build a query action with optional parameters.
    *
    * @return a function from ResultSet consumers to database actions.
    *
    * For example:
    * {{{
    * val q1 = DB.query("select 1 + ?", Some(Array("1"))
    * val addAction = q1 map (results =>
    *   new RsIterator(results)
    *     .map {r => r.getInt(1)}.to[Vector])
    * }
    * val answers = DbState.reader(src).run(addAction)
    * }}}
    */
  def query(q: String,
             params: Option[Array[String]] = None): DB[ResultSet] = {
    DB({conn: Connection => params match {
      case Some(ps) => {
        val s = conn.prepareStatement(q)
        for ((value, ix) <- ps zipWithIndex) {
          s.setString(ix + 1, value)
        }
        s.executeQuery()
      }
      case None => (conn.createStatement().executeQuery(q))
      }
    })
  }

  def chainQuery[T](q: String,
                    params: Option[Array[String]] = None,
                    chain: T): DB[(ResultSet, T)] = {
    query(q, params) map {results => (results, chain)}
  }

  //TODO: pushIn is limited to a Vector, ideally it would not have that limitation
  def pushIn[T](v: Vector[DB[T]]): DB[Vector[T]] = {
    def pushBind(x: DB[Vector[T]], y: DB[Vector[T]]): DB[Vector[T]] =
      x bind {xUnwrap => y bind {yUnwrap => DB(c => xUnwrap ++ yUnwrap)}}

    val prepInternalVectors: Vector[DB[Vector[T]]] =
      v map { db => db map { x => Vector(x)}}

    prepInternalVectors reduce pushBind
  }
}

/** A capability to make a Connection.
  *
  * ack: cribbed from functionaljava`fj.control.db.DB` in
  */
trait Connector {
  def connect(): Connection
  def connect(connectionProperties: Properties): Connection = connect()
}


/** Performs database I/O in order to read or write the database state.
  *
  * ack: cribbed from functionaljava`fj.control.db.DbState` in
  */
class DbState(pc: Connector, terminal: DB[Unit]) {

  /** Runs the given database action as a single transaction.
    *
    * @throws SQLException
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
/** DbState companion object
  * ack: cribbed from functionaljava`fj.control.db.DB` in
  */
object DbState {
  /** Connector for a database specified by URL.
    */
  def driverManager(url: String): Connector = {
    import java.sql.DriverManager

    new Connector {
      override def connect() : Connection = {
        DriverManager.getConnection(url)
      }
    };
  }

  /** Creates a database state reader.
    *
    * i.e. one whose terminal action is `rollback`.
    */
  def reader(pc: Connector) = new DbState(pc, rollback)
  def reader(url: String): DbState = reader(driverManager(url))


  /** Creates a database state writer.
    *
    * i.e. one whose terminal action is `commit`.
    */
  def writer(pc: Connector) = new DbState(pc, commit)
  def writer(url: String): DbState = writer(driverManager(url))

  private val rollback = DB( c => c.rollback())
  private val commit = DB(c => c.commit())
}

/** Connector from driver, url, username, password properties.
  *
  * @param name prefix to distinguish among sets of connection properties;
  *             e.g. `db1.url = ...`, `db2.url = ...`.
  */
class DBConfig(p: java.util.Properties, name: String) extends Connector {
  import java.sql.DriverManager

  /** Connect according to properties from constructor.
    */
  override def connect(): Connection = connect(new Properties)

  /** Connect according to combination of constructor and given properties.
    *
    * @throws SQLException
    */
  override def connect(connectionProperties: Properties):
      java.sql.Connection = {
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


/** Discoverable information about a table (or view)
  */
case class TableInfo(cat: String, schem: String, name: String, ty: String) {
  /** Format table info in SQL syntax.
    *
    * @param cols information discovered about this table's columns
    */
  def asSQL(cols: Vector[ColumnInfo]): String = {
    val colsAsSQL = cols map (_.asSQL)
    colsAsSQL.mkString(s"create $ty $schem.$name ( -- catalog: $cat\n",
      ",\n", ")\n")
  }
}
object TableInfo {
  /** Database action to discover table info.
    */
  def discover: DB[Vector[TableInfo]] = DB(c => {
    val allTableMetaData = new RsIterator(
      c.getMetaData().getTables(null, null, "%", null));

     (allTableMetaData map {rs: ResultSet =>
      TableInfo(
        rs.getString(1), // could fail? Try? map?
        rs.getString(2),
        rs.getString(3),
        rs.getString(4))
     }).to[Vector]
  })

  /** Discover table/column info and dump in SQL format.
    *
    * Note: skip tables in `INFORMATION_SCHEMA` and `sys`.
    */
  def dump(src: Connector, out: PrintStream,
    exclude: Array[String] = Array("INFORMATION_SCHEMA", "sys")) {
    catalog(src, exclude) foreach { case (table, cols) =>
      out.println(table.asSQL(cols))
    }
  }

  /** Discover table/column info.
    *
    */
  def catalog(src: Connector, exclude: Array[String]):
      Vector[(TableInfo, Vector[ColumnInfo])] = {

    def withColsQuery(ti: TableInfo): DB[(TableInfo, Vector[ColumnInfo])] = {
      // TOP 1 is a MS SQL ism
      val query = DB.chainQuery("SELECT TOP 1 * FROM " + ti.name, chain = ti)
      val resultsTransform = {resultsAndChain: (ResultSet, TableInfo) =>
        (resultsAndChain._2, Relation.domains(resultsAndChain._1))
      }
      query map resultsTransform
    }

    val withTableInfoCollectColumns =
      TableInfo.discover bind { tables => DB.pushIn(tables map withColsQuery) }

    DbState.reader(src).run(withTableInfoCollectColumns)
  }
}


/** Discoverable information about a column
  */
case class ColumnInfo(pos: Int, label: String,
  typeCode: Int, typeName: String, precision: Int) {
  /** Format column info as in SQL CREATE TABLE statement.
    */
  def asSQL(): String = s"  [$label] $typeName($precision) -- pos: $pos"
}


/** Provide convenient access to ResultSet
  */
object Relation {
  /** Discover [[ColumnInfo]] from query results.
    */
  def domains(results: ResultSet): Vector[ColumnInfo] = {
    val m = results.getMetaData() // ignore exceptions. hm.
    val cols = for (col <- 1 to m.getColumnCount()) yield ColumnInfo(col,
      m.getColumnLabel(col),
      m.getColumnType(col),
      m.getColumnTypeName(col),
      m.getPrecision(col))
    cols.to[Vector]
  }

  /** Map current record of ResultSet to (label, typeName, value) tuples.
    *
    * Value is taken from `ResultSet.getString`.
    */
  def record(results: ResultSet): Vector[(String, String, Option[String])] = {
    domains(results).zipWithIndex map { case (col, ix) =>
      (col.label, col.typeName, Option(results.getString(ix + 1)))
    }
  }

  /** Serialize current row of ResultSet in JSON format.
    *
    * Values of type `int` are not quoted; they are formatted as JSON numbers.
    * Other values (including dates) are quoted.
    *
    * TODO: double-check string quoting spec.
    * TODO: double-check DB schema for other types
    *
    * @see [[record]]
    */
  def asJSON(results: ResultSet): String = {
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

/** Treat a ResultSet as an Iterator.
  */
class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
  def hasNext: Boolean = rs.next()
  def next(): ResultSet = rs
}
