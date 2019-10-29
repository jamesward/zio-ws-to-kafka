import java.util.UUID

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import sttp.client._
import sttp.client.asynchttpclient.zio.ZioWebSocketHandler
import sttp.client.asynchttpclient.ziostreams.AsyncHttpClientZioStreamsBackend
import zio._
import zio.kafka.client.{Producer, ProducerSettings}
import zio.duration._
import zio.kafka.client.serde.Serde

object Main extends App {

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    wsToKafka.fold(_ => 1, _ => 0)
  }

  // todo: exit doesn't work
  val wsToKafka = system.env("BOOTSTRAP_SERVERS").flatMap { maybeBootstrapServers =>
    val producerSettings = ProducerSettings(
      bootstrapServers= List(maybeBootstrapServers.getOrElse("localhost:9092")),
      closeTimeout = 30.seconds,
      extraDriverSettings = Map.empty,
    )

    // todo: make[Any, _, _] is needed otherwise we get ZIO[Nothing with Blocking, _, _] and that doesn't work.
    Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string).use { producer =>
      ZIO.runtime[ZEnv].flatMap { runtime =>
        val sttpBackendTask = AsyncHttpClientZioStreamsBackend.usingConfigBuilder(runtime, _.setWebSocketMaxFrameSize(1024 * 1024))

        sttpBackendTask.flatMap { implicit sttpBackend =>
          ZioWebSocketHandler().flatMap { webSocketHandler =>
            basicRequest.get(uri"ws://stackoverflow-to-ws.default.35.224.5.101.nip.io/questions").openWebsocket(webSocketHandler).flatMap { response =>
              response.result.receiveText().flatMap {
                case Left(_) =>
                  ZIO.interrupt // todo: is this the right way to close?
                case Right(text) =>
                  // todo: not UUID for key - question URL?
                  val producerRecord = new ProducerRecord("stackoverflow-questions", UUID.randomUUID().toString, text.payload)
                  producer.produce(producerRecord)
              }.forever
            }
          }
        }
      }
    }
  }

}
