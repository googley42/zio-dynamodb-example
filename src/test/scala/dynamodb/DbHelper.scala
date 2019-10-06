package dynamodb

import java.net.URI
import java.util
import java.util.concurrent.CompletableFuture

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class ScalaPutItemRequest(tableName: String, private var map: Map[String, AttributeValue] = Map.empty) {
  def col(t: (String, String)): ScalaPutItemRequest = this.copy(map = this.map + (t._1 -> attrValue(t._2)))

  private def attrValue(value: String) =
    AttributeValue.builder().s(value).build()

  def cols(m: (String, String)*): ScalaPutItemRequest = this.copy(map = m.toMap map (t => t._1 -> attrValue(t._2)))

  def item: PutItemRequest =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(map.asJava)
      .build()
}

object DbHelper {
  type Row = util.Map[String, AttributeValue]
  val DateCutOff: String = "orderDate0"

  def createServer: DynamoDBProxyServer = {
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
    server
  }

  def createClient: DynamoDbAsyncClient = {
    val dynamodb = DynamoDbAsyncClient
      .builder()
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("http://localhost:8000"))
      .build()
    dynamodb
  }

  def createTableRequest: CreateTableRequest = {
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

  def findAllByIdInTheLastYear(
    dynamoDbClient: DynamoDbAsyncClient,
    limit: Int,
    id: String,
    lastEvaluatedKey: Row
  ): CompletableFuture[QueryResponse] = {
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
      .limit(limit)

    dynamoDbClient.query(
      qr.exclusiveStartKey(lastEvaluatedKey).build
    )
  }

}
