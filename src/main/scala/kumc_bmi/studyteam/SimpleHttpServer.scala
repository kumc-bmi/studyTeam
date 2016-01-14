package org.test.simplehttpserver

import java.net.InetSocketAddress
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import collection.mutable.HashMap

abstract class SimpleHttpServerBase(val socketAddress: String = "127.0.0.1",
                                    val port: Int = 8080,
                                    val backlog: Int = 0) extends HttpHandler {
  private val address = new InetSocketAddress(socketAddress, port)
  private val server = HttpServer.create(address, backlog)
  server.createContext("/", this)

  def redirect(url: String) =
    <html>
      <head>
          <meta http-equiv="Refresh" content={"0," + url}/>
      </head>
      <body>
        You are being redirected to:
        <a href={url}>
          {url}
        </a>
      </body>
    </html>

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

abstract class SimpleHttpServer extends SimpleHttpServerBase {
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

class HelloApp extends SimpleHttpServer {
  var count = 0

  get("/") {
    "There's nothing here"
  }

  get("/hello") {
    "Hello, world!"
  }

  get("/markup") {
    <html>
      <head>
        <title>Test Title</title>
      </head>
      <body>
        Test Body
      </body>
    </html>
  }

  def countPage = <html>
    <head>
      <title>Test Title</title>
    </head>
    <body>
      Count:
      {count}<a href="/increaseCount">++</a>
      <a href="/decreaseCount">--</a>
      <a href="/resetCount">Reset</a>
    </body>
  </html>

  get("/count") {
    countPage
  }

  get("/resetCount") {
    count = 0
    redirect("/count")
  }

  get("/increaseCount") {
    count = count + 1
    redirect("/count")
  }

  get("/decreaseCount") {
    count = count - 1
    redirect("/count")
  }

  get("/error") {
    throw new RuntimeException("Bad bad error occurred")
  }
}

object Main {

  def main(args: Array[String]) {
    val server = new HelloApp()
    server.start()
  }
}
