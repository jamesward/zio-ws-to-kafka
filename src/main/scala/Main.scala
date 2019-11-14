import java.util.UUID

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import sttp.client.ws.WebSocketEvent
import sttp.model.Uri
import sttp.model.ws.WebSocketFrame
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.kafka.client.serde.Serde
import zio.kafka.client.{Producer, ProducerSettings}
import zio.random.Random
import zio.system.System


/** Configuration for our web-service. */
case class Config(bootstrapServer: String, kafkaTopic: String, wsServer: Uri)

object Main extends App {
  type MyRecord = ProducerRecord[String, String]

  override def run(args: List[String]) = {
    wsToKafka.provideSome[ZEnv] { env =>
      new WS with Clock with Console with System with Random with Blocking {

        override val random: Random.Service[Any] = env.random
        override val clock: Clock.Service[Any] = env.clock
        override val system: System.Service[Any] = env.system
        override val console: Console.Service[Any] = env.console
        override val blocking: Blocking.Service[Any] = env.blocking

        override def ws: WS.Service[Any] = WS.Live.ws
      }
    }.fold(_ => 1, _ => 0)
  }

  val config: ZIO[system.System, Any, Config] = for {
    bootstrapServer <- system.env("BOOTSTRAP_SERVER").someOrFail()
    kafkaTopic <- system.env("KAFKA_TOPIC").someOrFail()
    wsServer <- system.env("WS_SERVER").someOrFail().flatMap { wsServer => ZIO.fromTry(Uri.parse(wsServer)) }
  } yield Config(bootstrapServer, kafkaTopic, wsServer)


  // todo: exit doesn't work
  val wsToKafka: ZIO[ZEnv with WS, Any, Nothing] = config.flatMap { config =>
    val producerSettings = ProducerSettings(
      bootstrapServers = List(config.bootstrapServer),
      closeTimeout = 30.seconds,
      extraDriverSettings = Map.empty,
    )
    // Note: Serde.string returns [Any,String] for serialization (i.e. serialization does not rely on environment).
    // However, we want to be more precise in our Environment usage, so we need to fully specify the type here.
    Producer.make[ZEnv, String, String](producerSettings, Serde.string, Serde.string).use(handleConnections(config))
  }

  def openWSConnection(uri: Uri): ZIO[zio.ZEnv with WS, Throwable, WSConnection] =
    ZIO.runtime[ZEnv].flatMap(runtime => ZIO.accessM[ZEnv with WS](_.ws.open(uri, runtime)))


  // This method takes in config + a kafka Producer, and will handle all incoming websocket connection, forever.
  // We return "Nothing" because we never return, infinitely handling connections.
  def handleConnections(config: Config)(producer: Producer[ZEnv, String, String]): ZIO[ZEnv with WS, Throwable, Nothing] =
    for {
      conn <- openWSConnection(config.wsServer)
      done <- conn.handleMessage(handleConnection(config.kafkaTopic, producer)).forever
    } yield done


  // Handles a websocket connection
  // TODO - look into hiding the Producer in the ZIO environment.
  def handleConnection(kafkaTopic: String, producer: Producer[ZEnv, String, String])(
    text: Either[WebSocketEvent.Close, WebSocketFrame.Text]): ZIO[ZEnv, Throwable, Task[RecordMetadata]] =
    text match {
      case Left(_) =>
        ZIO.interrupt // todo: is this the right way to close?
      case Right(text) =>
        // todo: not UUID for key - question URL?
        val producerRecord = new ProducerRecord(kafkaTopic, UUID.randomUUID().toString, text.payload)
        producer.produce(producerRecord)
    }
}
