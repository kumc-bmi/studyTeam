// http://groovy-lang.org/grape.html#Grape-JDBCDrivers
@GrabConfig(systemClassLoader=true)
@Grab(group='com.microsoft.sqlserver', module='mssql-jdbc', version='10.2.0.jre17')

import java.net.InetSocketAddress
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.logging.Logger
import static java.util.logging.Level.SEVERE
import javax.sql.DataSource

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer

import groovy.json.JsonOutput
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Immutable

@Immutable
@CompileStatic
class StudyTeam {
    private static Logger logger = Logger.getLogger("")

    public static void main(String[] args) {
        def fail = { ex -> System.err.println(ex); System.exit(1) }
        def dbAccess = { String u, Properties p -> DriverManager.getConnection(u, p) }
        def listen = { InetSocketAddress a, int b -> HttpServer.create(a, b) }
        CLI cli = new CLI(args)
        run(cli, System.out, fail, dbAccess, listen)
    }

    public static void run(CLI cli, PrintStream out,
                           Closure fail,
                           Closure<Connection> dbAccess,
                           Closure<HttpServer> listen) {
        def config = new Properties()
        try {
            cli.arg('--config') { String fn ->
                config = getConfig(new File(fn))
            }
        } catch (java.io.FileNotFoundException ex) { fail(ex) }

        def src = new DBConfig(config: config, name: "src", dbAccess: dbAccess)

        try {
            cli.arg("--by-id") { String id ->
                byId(src, id, out)
            }
        } catch (java.sql.SQLException ex) { fail(ex) }

        if (cli.flag("--serve")) {
            String host = config.getProperty("http.host") ?: "127.0.0.1"

            def port = (config.getProperty("http.port") ?: "8080") as int
            logger.info("serving at http://$host:$port")
            new StudyServer(src: src, socketAddress: host, port: port).start(listen)
        }
    }

    static Properties getConfig(File configFile) {
        Properties properties = new Properties()
        configFile.withInputStream {
            properties.load(it)
        }
        properties
    }

    static void byId(DataSource src, String id, PrintStream out) {
        out.println("Members of study with ID=$id:")
        def info = team(src, id)
        out.println(JsonOutput.prettyPrint(JsonOutput.toJson(info)))
    }

    static List team(DataSource src, String id) {
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



@CompileStatic
class CLI {
    String[] args
    
    CLI(args_given) {          
        this.args = args_given
    }

    boolean flag(String it) {
        args.contains(it)
    }
    
    def arg(String sentinel, Closure thunk) {
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
@Immutable
@CompileStatic
class DBConfig extends NoopDataSource {
    Properties config
    String name
    Closure dbAccess
    static private Logger logger = Logger.getLogger("")

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

    Logger getParentLogger() {
        logger
    }
}


@CompileStatic
abstract class NoopDataSource implements DataSource {
    abstract Connection getConnection()

    Connection getConnection(String username, String password) {
        getConnection()
    }

    boolean isWrapperFor(Class iface) {
        false
    }

    PrintWriter getLogWriter() {
        null
    }

    Logger getParentLogger() {
        throw new SQLFeatureNotSupportedException()
    }

    void setLogWriter(java.io.PrintWriter out) {
        throw new SQLException()
    }

    int getLoginTimeout() {
        0
    }

    void setLoginTimeout(int t) {
        throw new SQLException()
    }


    Object unwrap(Class t) {
        throw new SQLException()
    }

}


/** HTTP service to look up IRB study team info by study id.
 *
 * @param port server listening port
 * @param src [[Connector]] to e-compliance DB.
 */
@CompileStatic
class StudyServer extends SimpleHttpServerBase {
    private Logger logger = Logger.getLogger("")

    DataSource src

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
    void handle(HttpExchange exchange) {
        String query = exchange.requestURI.query

        def m = query =~ /id=(.*)/
        if (m.find()) {
            String id = m.group(1)
            logger.info("GET id=$id")
            List info
            try {
                info = StudyTeam.team(src, id)
            } catch (java.sql.SQLException ex) {
                logger.log(SEVERE, ex.toString())
                respond(exchange, 500, "cannot access team membership")
                return
            }
            logger.info("protocol $id has ${info.size()} study team members")
            def text = JsonOutput.prettyPrint(JsonOutput.toJson(info))
            respond(exchange, 200, text)
        } else {
            respond(exchange, 404, "")
        }
    }
}
