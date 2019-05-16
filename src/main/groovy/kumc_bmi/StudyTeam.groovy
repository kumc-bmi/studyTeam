// http://groovy-lang.org/grape.html#Grape-JDBCDrivers
@GrabConfig(systemClassLoader=true)
@Grab(group='com.microsoft.sqlserver', module='mssql-jdbc', version='7.2.0.jre8')

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.logging.Logger
import javax.sql.DataSource

import groovy.sql.Sql
import groovy.json.JsonOutput


class StudyTeam {
    public static void main(String[] args) {
        def fail = { ex -> System.err.println(ex); System.exit(1) }
        def dbAccess = { u, p -> DriverManager.getConnection(u, p) }
        run(new CLI(args: args), System.out, fail, dbAccess)
    }

    public static void run(cli, out, fail, dbAccess) {
        def config = new Properties()
        try {
            cli.arg('--config') { fn ->
                config = getConfig(new File(fn))
            }
        } catch (java.io.FileNotFoundException ex) { fail(ex) }

        def src = new DBConfig(config: config, name: "src",
                               dbAccess: { u, p -> DriverManager.getConnection(u, p) })

        try {
            cli.arg("--by-id") { id ->
                byId(src, id, System.out)
            }
        } catch (java.sql.SQLException ex) { fail(ex) }
    }

    static Properties getConfig(File configFile) {
        Properties properties = new Properties()
        configFile.withInputStream {
            properties.load(it)
        }
        properties
    }

    static def byId(src, id, out) {
        out.println("Members of study with ID=$id:")
        def info = team(src, id)
        out.println(JsonOutput.prettyPrint(JsonOutput.toJson(info)))
    }

    static def team(DataSource src, String id) {
        def sql = new Sql(src)
        def q = """
            select p.ID as [Employee ID], p.userId, p.accountDisabled
                 , p.lastName, p.firstName, p.EmailPreferred, p.BusinesPhone
                 , irb.ID, irb.State, irb.[Date Expiration], irb.[Full Study Title]
            from _studyTeamMemberInfo tm
            join KU_IRBSubmissionView irb on irb.ParentStudyOid = tm.owningEntity
            join KU_PersonView p on p.OID = tm.[studyTeamMember.oid]
            where irb.ID = :id
        """

        sql.rows(q, [id: id])
    }
}

class CLI {
    String[] args

    def flag = { args.contains(it) }
    def arg = { String sentinel, thunk ->
        def i = (args as List).findIndexOf { it == sentinel }
        def l = args.length
        if (i >= 0 && i + 1 < l) {
            thunk(args[i + 1]);
        } else if (i >= 0) {
            System.err.println("$sentinel requires value argument")
        }
    }
}


/** Connector from driver, url, username, password properties.
 *
 * @param name prefix to distinguish among sets of connection properties;
 *             e.g. `db1.url = ...`, `db2.url = ...`.
 */
class DBConfig extends NoopDataSource {
    Properties config
    String name
    Closure dbAccess
    // Logger logger = Logger.getLogger("")

    String prop(String p) {
        def val = config.getProperty(name + p)
        if (val == null) {
            throw new SQLException("missing property: ${name + p}")
        }
        val
    }

    Connection getConnection() {
        def driver = prop(".driver")
        def url = prop(".url")
        def connectionProperties = new Properties()
        // sloppy in-place update
        connectionProperties.put("user", prop(".username"))
        connectionProperties.put("password", prop(".password"))
        Class.forName(driver)
        // throws an exception. hmm.
        dbAccess(url, connectionProperties)
    }
}


abstract class NoopDataSource implements DataSource {
    abstract Connection getConnection()

    Connection getConnection(String username, String password) {
        getConnection()
    }

    boolean isWrapperFor(Class iface) {
        false
    }

    PrintWriter getLogWriter() {
        throw new SQLException()
    }

    Logger getParentLogger() {
        throw new SQLFeatureNotSupportedException()
    }

    void setLogWriter(java.io.PrintWriter out) {
        throw new SQLException()
    }

    int getLoginTimeout() {
        throw new SQLException()
    }

    void setLoginTimeout(int t) {
        throw new SQLException()
    }


    Object unwrap(Class t) {
        throw new SQLException()
    }

}
