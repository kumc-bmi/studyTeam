// http://groovy-lang.org/grape.html#Grape-JDBCDrivers
@GrabConfig(systemClassLoader=true)
@Grab(group='com.microsoft.sqlserver', module='mssql-jdbc', version='7.2.0.jre8')

import java.sql.Connection
import java.sql.DriverManager

import groovy.sql.Sql



class StudyTeam {
    static Properties getConfig(File configFile) {
        Properties properties = new Properties()
        configFile.withInputStream {
            properties.load(it)
        }
        properties
    }

    public static void main(String[] args) {
        def flag = { args.contains(it) }
        def arg = { String sentinel, String fallback ->
            def i = (args as List).findIndexOf { it == sentinel }
            def l = args.length
            if (i >= 0 && i + 1 < l) {
                return args[i + 1];
            } else if (i >= 0) {
                System.err.println("$sentinel requires value argument")
            }
            return fallback
        }

        def config
        try {
            config = getConfig(new File(arg('--config', 'db.properties')))
        } catch (java.io.FileNotFoundException fail) {
            System.err.println(fail)
            System.exit(1)
        }
        
        def src = new DBConfig(config: config, name: "src")
        src.withConnection() { sql ->
        }
    }
}


/** A capability to make a Connection.
  */
interface Connector {
    void withConnection(Closure<Connection> thunk)
}

/** Connector from driver, url, username, password properties.
  *
  * @param name prefix to distinguish among sets of connection properties;
  *             e.g. `db1.url = ...`, `db2.url = ...`.
  */
class DBConfig implements Connector {
    Properties config
    String name

    void withConnection(Closure<Connection> thunk) {
        def driver = config.getProperty(name + ".driver")
        def url = config.getProperty(name + ".url")
        def connectionProperties = new Properties()
        // sloppy in-place update
        connectionProperties.put("user", config.getProperty(name + ".username"))
        connectionProperties.put("password", config.getProperty(name + ".password"))
        Class.forName(driver)
        // throws an exception. hmm.
        def conn = DriverManager.getConnection(url, connectionProperties)
        thunk(conn)
        conn.close()
    }
}
