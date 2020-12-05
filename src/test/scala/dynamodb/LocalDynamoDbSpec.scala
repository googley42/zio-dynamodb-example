//package dynamodb
//
//import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
//import dynamodb.DbHelper.Row
//import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
//import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
//import software.amazon.awssdk.services.dynamodb.model.{PutItemRequest, PutItemResponse, QueryResponse}
//import zio._
//import zio.console.Console
//import zio.stream._
//
//import scala.collection.JavaConverters._
//
//class LocalDynamoDbSpec extends WordSpec with Matchers with BeforeAndAfterAll {
//  private lazy val server: DynamoDBProxyServer = DbHelper.createServer
//
//  override def beforeAll(): Unit =
//    server.start()
//
//  override def afterAll(): Unit = server.stop()
//
//  "zio" should {
//
//    "stream dynamoDb table" in {
//
//      val dynamodb: DynamoDbAsyncClient = DbHelper.createClient
//
//      val completableFuture = dynamodb.createTable(DbHelper.createTableRequest)
//      val zioCreateTable = ZIO.fromCompletionStage(completableFuture)
//
//      val zioPutItems: ZIO[Any, Throwable, List[PutItemResponse]] = ZIO.foreach(1 to 5) { i =>
//        val item: PutItemRequest = ScalaPutItemRequest(tableName = "Entitlement")
//          .cols(
//            "id" -> "id",
//            "entitlement" -> s"entitlement$i",
//            "orderDate" -> s"orderDate$i",
//            "accountId" -> s"1234abcd"
//          )
//          .item
//        ZIO.fromCompletionStage(dynamodb.putItem(item))
//      }
//
//      val program: ZIO[Console, Throwable, Int] = for {
//        _ <- zioCreateTable
//        _ <- zioPutItems
//        processedCount <- streamWithJavaInterop(dynamodb)
//          .run(ZSink.foldLeftM(0) { (s: Int, a: Row) =>
//            // constant memory space processing here
//            console.putStrLn("Sink >>> " + a.get("orderDate")) *> UIO.effectTotal(s + 1)
//          })
//      } yield processedCount
//
//      val processedCount = Runtime.default.unsafeRun(program)
//
//      processedCount shouldBe 5
//    }
//  }
//
//  // Lifts CompletionStage to a ZIO Task using the ZIO java future interop module
//  def streamWithJavaInterop(client: DynamoDbAsyncClient): ZStream[Console, Throwable, Row] = {
//    val lekStart = QueryResponse.builder().build().lastEvaluatedKey()
//    val stream: ZStream[Console, Throwable, QueryResponse] = Stream.unfoldM(lekStart) { lek =>
//      val task: Task[QueryResponse] = // constant memory space processing here
//        ZIO.fromCompletionStage(DbHelper.findAllByIdInTheLastYear(client, limit = 3, "id", lek))
//      task
//        .map { qr =>
//          Some((qr, qr.lastEvaluatedKey()))
//        }
//    }
//    stream
//      .tap(_ => console.putStrLn("Stream >>> fetched a batch ot items"))
//      .takeUntil { qr =>
//        qr.items.size == 0 || qr.lastEvaluatedKey.size == 0
//      }
//      .flatMap(qr => Stream.fromIterable(qr.items.asScala))
//  }
//
//  // we can manually lift CompletionStage to a ZIO Task as well
//  def streamWithManualJavaInterop(client: DynamoDbAsyncClient): ZStream[Console, Throwable, Row] = {
//    val lekStart = QueryResponse.builder().build().lastEvaluatedKey()
//    val stream: Stream[Throwable, QueryResponse] = Stream.unfoldM(lekStart) { lek =>
//      Task
//        .effectAsync[QueryResponse] { cb => // constant memory space processing here
//          DbHelper
//            .findAllByIdInTheLastYear(client, limit = 3, "id", lek)
//            .handle[Unit]((result, err) => {
//              err match {
//                case null =>
//                  cb(IO.succeed(result))
//                case ex =>
//                  cb(IO.fail(ex))
//              }
//            })
//          ()
//        }
//        .map { qr =>
//          Some((qr, qr.lastEvaluatedKey()))
//        }
//    }
//    stream
//      .tap(_ => console.putStrLn("Stream >>> fetched a batch ot items"))
//      .takeUntil { qr =>
//        qr.items.size == 0 || qr.lastEvaluatedKey.size == 0
//      }
//      .flatMap(qr => Stream.fromIterable(qr.items.asScala))
//  }
//
//}
