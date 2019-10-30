import java.util.UUID

import org.apache.kafka.clients.producer.ProducerRecord
import sttp.client._
import sttp.client.asynchttpclient.zio.ZioWebSocketHandler
import sttp.client.asynchttpclient.ziostreams.AsyncHttpClientZioStreamsBackend
import sttp.model.Uri
import zio._
import zio.kafka.client.{Producer, ProducerSettings}
import zio.duration._
import zio.kafka.client.serde.Serde

object Main extends App {

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    wsToKafka.fold(_ => 1, _ => 0)
  }

  case class Config(bootstrapServer: String, kafkaTopic: String, wsServer: Uri)

  val config = for {
    bootstrapServer <- system.env("BOOTSTRAP_SERVER").someOrFail()
    kafkaTopic <- system.env("KAFKA_TOPIC").someOrFail()
    wsServer <- system.env("WS_SERVER").someOrFail().flatMap { wsServer => ZIO.fromTry(Uri.parse(wsServer)) }
  } yield Config(bootstrapServer, kafkaTopic, wsServer)

  // todo: exit doesn't work
  val wsToKafka = config.flatMap { config =>
    val producerSettings = ProducerSettings(
      bootstrapServers= List(config.bootstrapServer),
      closeTimeout = 30.seconds,
      extraDriverSettings = Map.empty,
    )

    // todo: make[Any, _, _] is needed otherwise we get ZIO[Nothing with Blocking, _, _] and that doesn't work.
    Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string).use { producer =>
      ZIO.runtime[ZEnv].flatMap { runtime =>
        val sttpBackendTask = AsyncHttpClientZioStreamsBackend.usingConfigBuilder(runtime, _.setWebSocketMaxFrameSize(1024 * 1024))

        sttpBackendTask.flatMap { implicit sttpBackend =>
          ZioWebSocketHandler().flatMap { webSocketHandler =>
            basicRequest.get(config.wsServer).openWebsocket(webSocketHandler).flatMap { response =>
              response.result.receiveText().flatMap {
                case Left(_) =>
                  ZIO.interrupt // todo: is this the right way to close?
                case Right(text) =>
                  // todo: not UUID for key - question URL?
                  val producerRecord = new ProducerRecord(config.kafkaTopic, UUID.randomUUID().toString, text.payload)
                  producer.produce(producerRecord)
              }.forever
            }
          }
        }
      }
    }
  }

}
