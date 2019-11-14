import sttp.client.asynchttpclient.zio.ZioWebSocketHandler
import sttp.client.asynchttpclient.ziostreams.AsyncHttpClientZioStreamsBackend
import sttp.client.basicRequest
import sttp.client.ws.{WebSocket, WebSocketEvent, WebSocketResponse}
import sttp.model.Uri
import sttp.model.ws.WebSocketFrame
import zio._

trait WS {
  def ws: WS.Service[Any]
}

object WS {

  trait Service[R] {
    def open(url: Uri, runtime: Runtime[ZEnv]): ZIO[R, Throwable, WSConnection]
  }

  object Live extends WS {
    override def ws: Service[Any] = new WS.Service[Any] {
      override def open(url: Uri, runtime: Runtime[ZEnv]): ZIO[Any, Throwable, WSConnection] = {
        val sttpBackendTask = AsyncHttpClientZioStreamsBackend.usingConfigBuilder(runtime, _.setWebSocketMaxFrameSize(1024 * 1024))
        for {
          sttpBackend <- sttpBackendTask
          webSocketHandler <- ZioWebSocketHandler()
          response <- basicRequest.get(url).openWebsocket(webSocketHandler)(sttpBackend, implicitly)
        } yield WSConnection(response)
      }
    }
  }

}

trait WSConnection {
  def handleMessage[E, T](f: Either[WebSocketEvent.Close, WebSocketFrame.Text] => ZIO[E, Throwable, T]): ZIO[E, Throwable, T]
}

object WSConnection {

  def apply(response: WebSocketResponse[WebSocket[Task]]): WSConnection = new WSConnection {
    override def handleMessage[E, T](f: Either[WebSocketEvent.Close, WebSocketFrame.Text] => ZIO[E, Throwable, T]): ZIO[E, Throwable, T] =
      response.result.receiveText().flatMap(f)
  }
}


