import java.util.UUID

import org.apache.kafka.clients.producer.{
  ProducerRecord, 
  RecordMetadata
}
import sttp.client._
import sttp.client.asynchttpclient.zio.ZioWebSocketHandler
import sttp.client.asynchttpclient.ziostreams.AsyncHttpClientZioStreamsBackend
import sttp.model.Uri
import zio._
import zio.blocking.Blocking
import zio.kafka.client.{Producer, ProducerSettings}
import zio.duration._
import zio.kafka.client.serde.Serde
import sttp.client.ws.{
  WebSocketResponse,
  WebSocketEvent
}
import sttp.model.ws.WebSocketFrame


/** Configuration for our web-service. */
case class Config(bootstrapServer: String, kafkaTopic: String, wsServer: Uri)

object Main extends App {
  type MyRecord = ProducerRecord[String, String]

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    wsToKafka.fold(_ => 1, _ => 0)
  }

  val config = for {
    bootstrapServer <- system.env("BOOTSTRAP_SERVER").someOrFail()
    kafkaTopic <- system.env("KAFKA_TOPIC").someOrFail()
    wsServer <- system.env("WS_SERVER").someOrFail().flatMap { wsServer => ZIO.fromTry(Uri.parse(wsServer)) }
  } yield Config(bootstrapServer, kafkaTopic, wsServer)


  // todo: exit doesn't work
  val wsToKafka: ZIO[ZEnv, Any, Nothing] = config.flatMap { config =>
    val producerSettings = ProducerSettings(
      bootstrapServers= List(config.bootstrapServer),
      closeTimeout = 30.seconds,
      extraDriverSettings = Map.empty,
    )

    // todo: make[Any, _, _] is needed otherwise we get ZIO[Nothing with Blocking, _, _] and that doesn't work.
    Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string).use(handleConnections(config))
  }

  // This method takes in config + a kafka Producer, and will handle all incoming websocket connection, forever.
  // We return "Nothing" because we never return, infinitely handling connections.
  def handleConnections(config: Config)(producer: Producer[Any, String, String]): ZIO[ZEnv, Throwable, Nothing] =
    for {
        runtime <- ZIO.runtime[ZEnv]
        sttpBackendTask = AsyncHttpClientZioStreamsBackend.usingConfigBuilder(runtime, _.setWebSocketMaxFrameSize(1024 * 1024))
        sttpBackend <- sttpBackendTask
        webSocketHandler <- ZioWebSocketHandler()
        response <- basicRequest.get(config.wsServer).openWebsocket(webSocketHandler)(sttpBackend, implicitly)
        text <- response.result.receiveText()
        result <- handleConnection(config.kafkaTopic, producer)(text).forever
      } yield result


  // Handles a websocket connection
  // TODO - look into hiding the Producer in the ZIO environment.
  def handleConnection(kafkaTopic: String, producer: Producer[Any, String, String])(
    text: Either[WebSocketEvent.Close,WebSocketFrame.Text]): ZIO[Any with Blocking, Throwable, Task[RecordMetadata]] =
      text match {
        case Left(_) =>
          ZIO.interrupt // todo: is this the right way to close?
        case Right(text) =>
          // todo: not UUID for key - question URL?
          val producerRecord = new ProducerRecord(kafkaTopic, UUID.randomUUID().toString, text.payload)
          producer.produce(producerRecord)
      }
}
