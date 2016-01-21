package kumc_bmi.studyteam

import java.net.InetSocketAddress
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

/** HTTP server using the HttpServer class that is built-in in JDK6.
  *
  *  ack: re "Bootstrapping a web server in Scala"
  *  [[http://stackoverflow.com/a/6432180 Tommy Jun 21 '11 ]]
  */
abstract class SimpleHttpServerBase(val socketAddress: String = "127.0.0.1",
                                    val port: Int = 8080,
                                    val backlog: Int = 0) extends HttpHandler {
  private val address = new InetSocketAddress(socketAddress, port)
  private val server = HttpServer.create(address, backlog)
  server.createContext("/", this)

  def respond(exchange: HttpExchange, code: Int = 200, body: String = "") {
    val bytes = body.getBytes
    exchange.sendResponseHeaders(code, bytes.size)
    exchange.getResponseBody.write(bytes)
    exchange.getResponseBody.write("\r\n\r\n".getBytes)
    exchange.getResponseBody.close()
    exchange.close()
  }

  def start() = server.start()

  def stop(delay: Int = 1) = server.stop(delay)
}

/** HTTP Server with mappings from paths to handlers
  *
  * NOTE: for documentation purposes only; not used by [[StudyTeam]].
  */
abstract class SimpleHttpServer extends SimpleHttpServerBase {
  import collection.mutable.HashMap
  private val mappings = new HashMap[String, () => Any]

  def get(path: String)(action: => Any) = mappings += path -> (() => action)

  def handle(exchange: HttpExchange) = mappings.get(exchange.getRequestURI.getPath) match {
    case None => respond(exchange, 404)
    case Some(action) => try {
      respond(exchange, 200, action().toString)
    } catch {
      case ex: Exception => respond(exchange, 500, ex.toString)
    }
  }
}

/** Example handler-based HTTP server.
  *
  * NOTE: for documentation purposes only; not used by [[StudyTeam]].
  */
class HelloApp extends SimpleHttpServer {
  get("/hello") {
    "Hello, world!"
  }
}
