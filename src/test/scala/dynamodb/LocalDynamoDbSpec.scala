package dynamodb

import java.net.URI
import java.util
import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, CreateTableRequest, KeySchemaElement, KeyType, LocalSecondaryIndex, Projection, ProjectionType, ProvisionedThroughput, PutItemRequest, QueryRequest, QueryResponse, ScalarAttributeType, Select}
import zio.{DefaultRuntime, IO, Ref, Task, UIO, ZIO}
import zio.interop.javaz._
import zio.stream._

import scala.collection.JavaConverters._

class LocalDynamoDbSpec extends WordSpec with Matchers with BeforeAndAfterAll with DefaultRuntime {
  private var server: DynamoDBProxyServer = _
  type Row = util.Map[String, AttributeValue]

  override def beforeAll(): Unit = {
    System.setProperty("sqlite4java.library.path", "native-libs")
    System.setProperty("aws.accessKeyId", "dummy-key")
    System.setProperty("aws.secretKey", "dummy-key") // This is not used
    System.setProperty("aws.secretAccessKey", "dummy-key")

    import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
    import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
    // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP

    val localArgs = Array("-inMemory")
    var server: DynamoDBProxyServer = null
    server = ServerRunner.createServerFromCommandLineArgs(localArgs)
    server.start()
  }

  override def afterAll(): Unit =
    if (server != null)
      server.stop()

  "local db" should {

    "create table Entitlement" in {

      val dynamodb: DynamoDbAsyncClient = createClient

      val cf = dynamodb.createTable(createTableRequest)
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
            (s + 1) //TODO: putStrLn
          })
      } yield processedCount

      val processedCount = unsafeRun(program)

      processedCount shouldBe 5
    }
  }

  private def createClient = {
    val dynamodb = DynamoDbAsyncClient
      .builder()
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("http://localhost:8000"))
      .build()
    dynamodb
  }

  def streamWithManualTaskEffectAsync(client: DynamoDbAsyncClient): ZStream[Any, Throwable, Row] = {
    val lekStart = QueryResponse.builder().build().lastEvaluatedKey()
    val stream: Stream[Throwable, QueryResponse] = Stream.unfoldM(lekStart) { lek =>
      Task
        .effectAsync[QueryResponse] { cb =>
          findAllByIdInTheLastYear(client, "id", Some(lek)).handle[Unit]((result, err) => {
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
        ZIO.fromCompletionStage(UIO.effectTotal(findAllByIdInTheLastYear(client, "id", Some(lek))))
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

  val DateCutOff: String = "orderDate0"

  private def findAllByIdInTheLastYear(
    dynamoDbClient: DynamoDbAsyncClient,
    id: String,
    lastEvaluatedKey: Option[Row] = None
  ) = {
    val qr = QueryRequest
      .builder()
      .select(Select.ALL_ATTRIBUTES)
      .tableName("Entitlement")
      .indexName("idOrderDate")
      .keyConditionExpression("id = :id AND orderDate >= :orderDate")
      .expressionAttributeValues(
        Map(
          ":id" -> AttributeValue.builder().s(id).build(),
          ":orderDate" -> AttributeValue.builder().s(DateCutOff).build()
        ).asJava
      )
      .limit(3)

    dynamoDbClient.query(
      lastEvaluatedKey.fold(qr)(lek => qr.exclusiveStartKey(lek)).build
    )
  }

  private def createTableRequest: CreateTableRequest = {
    implicit def attrDef(t: (String, ScalarAttributeType)): AttributeDefinition =
      AttributeDefinition.builder.attributeName(t._1).attributeType(t._2).build
    implicit def keySchemaElmt(t: (String, KeyType)): KeySchemaElement =
      KeySchemaElement.builder.attributeName(t._1).keyType(t._2).build
    implicit def seqAsJava[T](seq: Seq[T]): util.List[T] = seq.asJava

    val tableName = "Entitlement"
    val hashKeyName = "id"
    val attributeDefinitions: Seq[AttributeDefinition] = Seq(
      hashKeyName -> ScalarAttributeType.S,
      "entitlement" -> ScalarAttributeType.S,
      "orderDate" -> ScalarAttributeType.S
    )

    val ks: Seq[KeySchemaElement] = Seq(
      hashKeyName -> KeyType.HASH,
      "entitlement" -> KeyType.RANGE
    )
    val localKs: Seq[KeySchemaElement] = Seq(
      hashKeyName -> KeyType.HASH,
      "orderDate" -> KeyType.RANGE
    )
    val localSecIndex: LocalSecondaryIndex = LocalSecondaryIndex.builder
      .indexName("idOrderDate")
      .keySchema(localKs)
      .projection(Projection.builder.projectionType(ProjectionType.ALL).build)
      .build
    val provisionedThroughput = ProvisionedThroughput.builder
      .readCapacityUnits(5L)
      .writeCapacityUnits(5L)
      .build
    val request = CreateTableRequest.builder
      .tableName(tableName)
      .attributeDefinitions(attributeDefinitions)
      .keySchema(ks)
      .localSecondaryIndexes(localSecIndex)
      .provisionedThroughput(provisionedThroughput)
      .build
    request
  }

}

case class ScalaPutItemRequest(tableName: String, private var map: Map[String, AttributeValue] = Map.empty) {
  def col(t: (String, String)): ScalaPutItemRequest = this.copy(map = this.map + (t._1 -> attrValue(t._2)))

  private def attrValue(value: String) =
    AttributeValue.builder().s(value).build()

  def cols(m: (String, String)*) = this.copy(map = m.toMap map (t => t._1 -> attrValue(t._2)))

  def item =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(map.asJava)
      .build()
}
