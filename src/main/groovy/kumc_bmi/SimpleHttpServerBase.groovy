import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.logging.Logger

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer

import groovy.transform.CompileStatic
import groovy.transform.Immutable

/** HTTP server using the HttpServer class that is built-in in JDK6.
  *
  *  ack: re "Bootstrapping a web server in Scala"
  *  [[http://stackoverflow.com/a/6432180 Tommy Jun 21 '11 ]]
  */
@CompileStatic
abstract class SimpleHttpServerBase implements HttpHandler {
    String socketAddress
    int port

    private HttpServer server

    def respond(HttpExchange exchange, int code, String body) {
        def bytes = body.getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(code, bytes.size())
        def out = exchange.getResponseBody()
        out.write(bytes)
        out.write("\r\n\r\n".getBytes(StandardCharsets.UTF_8))
        out.close()
        exchange.close()
    }

    def start() {
        InetSocketAddress address = new InetSocketAddress(socketAddress, port)
        int backlog = 0

        server = HttpServer.create(address, backlog)
        server.createContext("/", this)
        server.start()
    }

    def stop(int delay) { server.stop(delay) }
}
