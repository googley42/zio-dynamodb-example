package dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import dynamodb.DbHelper.Row
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{PutItemRequest, QueryResponse}
import zio._
import zio.interop.javaz._
import zio.stream._

import scala.collection.JavaConverters._

class LocalDynamoDbSpec extends WordSpec with Matchers with BeforeAndAfterAll with DefaultRuntime {
  private lazy val server: DynamoDBProxyServer = DbHelper.createServer

  override def beforeAll(): Unit =
    server.start()

  override def afterAll(): Unit = server.stop()

  "zio" should {

    "stream dynamodb table" in {

      val dynamodb: DynamoDbAsyncClient = DbHelper.createClient

      val cf = dynamodb.createTable(DbHelper.createTableRequest)
      val zioCreateTable = ZIO.fromCompletionStage(UIO.effectTotal(cf))

      val zioPutItems = ZIO.foreach(1 to 5) { i =>
        val item: PutItemRequest = ScalaPutItemRequest(tableName = "Entitlement")
          .cols(
            "id" -> "id",
            "entitlement" -> s"entitlement$i",
            "orderDate" -> s"orderDate$i",
            "accountId" -> s"1234abcd"
          )
          .item
        ZIO.fromCompletionStage(UIO.effectTotal(dynamodb.putItem(item)))
      }

      val program: ZIO[Any, Throwable, Int] = for {
        _ <- zioCreateTable
        _ <- zioPutItems
        processedCount <- streamWithJavaInterop(dynamodb)
          .run(ZSink.foldLeft(0) { (s: Int, a: Row) =>
            println("XXXXXXXXXXXXXXX " + a.get("orderDate"))
            s + 1 //TODO: putStrLn
          })
      } yield processedCount

      val processedCount = unsafeRun(program)

      processedCount shouldBe 5
    }
  }

  def streamWithManualTaskEffectAsync(client: DynamoDbAsyncClient): ZStream[Any, Throwable, Row] = {
    val lekStart = QueryResponse.builder().build().lastEvaluatedKey()
    val stream: Stream[Throwable, QueryResponse] = Stream.unfoldM(lekStart) { lek =>
      Task
        .effectAsync[QueryResponse] { cb =>
          DbHelper
            .findAllByIdInTheLastYear(client, "id", Some(lek))
            .handle[Unit]((result, err) => {
              err match {
                case null =>
                  cb(IO.succeed(result))
                case ex =>
                  cb(IO.fail(ex))
              }
            })
          ()
        }
        .map { qr =>
          println("XXXXXXXXXXXXXXXXXXX about to fetch a batch ot items")
          Some((qr, qr.lastEvaluatedKey()))
        }
    }
    stream
      .takeUntil { qr =>
        qr.items.size == 0 || qr.lastEvaluatedKey.size == 0
      }
      .flatMap(qr => Stream.fromIterable(qr.items.asScala))
  }

  def streamWithJavaInterop(client: DynamoDbAsyncClient): Stream[Throwable, Row] = {
    val lekStart = QueryResponse.builder().build().lastEvaluatedKey()
    val stream: Stream[Throwable, QueryResponse] = Stream.unfoldM(lekStart) { lek =>
      val task: Task[QueryResponse] =
        ZIO.fromCompletionStage(UIO.effectTotal(DbHelper.findAllByIdInTheLastYear(client, "id", Some(lek))))
      task
        .map { qr =>
          println("XXXXXXXXXXXXXXXXXXX about to fetch a batch ot items")
          Some((qr, qr.lastEvaluatedKey()))
        }
    }
    stream
      .takeUntil { qr =>
        qr.items.size == 0 || qr.lastEvaluatedKey.size == 0
      }
      .flatMap(qr => Stream.fromIterable(qr.items.asScala))
  }

}
