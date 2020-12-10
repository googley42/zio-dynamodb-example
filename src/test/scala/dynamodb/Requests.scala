package dynamodb

import io.github.vigoo.zioaws.dynamodb.model
import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import io.github.vigoo.zioaws.dynamodb.model._

object Requests {
  val lekStart = Option.empty[Map[AttributeName, AttributeValue]]
  val hashKeyName = "id"
  val DateCutOff: String = "orderDate0"

  val createTableRequest: CreateTableRequest = CreateTableRequest(
    attributeDefinitions = Seq(
      AttributeDefinition(hashKeyName, ScalarAttributeType.S),
      AttributeDefinition("entitlement", ScalarAttributeType.S),
      AttributeDefinition("orderDate", ScalarAttributeType.S)
    ),
    tableName = "Entitlement",
    keySchema =
      Seq(model.KeySchemaElement(hashKeyName, KeyType.HASH), model.KeySchemaElement("entitlement", KeyType.RANGE)),
    localSecondaryIndexes = Some(
      Seq(
        LocalSecondaryIndex(
          "idOrderDate",
          Seq(KeySchemaElement(hashKeyName, KeyType.HASH), KeySchemaElement("orderDate", KeyType.RANGE)),
          projection = Projection(projectionType = Some(ProjectionType.ALL))
        )
      )
    ),
    provisionedThroughput = Some(ProvisionedThroughput(5L, 5L))
  )

  def putItemRequest(i: Int): PutItemRequest =
    PutItemRequest(
      tableName = "Entitlement",
      item = Map(
        "id" -> AttributeValue(s = Some("id")),
        "entitlement" -> AttributeValue(s = Some(s"entitlement$i")),
        "orderDate" -> AttributeValue(s = Some(s"orderDate$i")),
        "accountId" -> AttributeValue(s = Some(s"1234abcd"))
      )
    )

  def findAllByIdInTheLastYear(id: String): QueryRequest =
    QueryRequest(
      tableName = "Entitlement",
      indexName = Some("idOrderDate"),
      keyConditionExpression = Some("id = :id AND orderDate >= :orderDate"),
      select = Some(Select.ALL_ATTRIBUTES),
      expressionAttributeValues =
        Some(Map(":id" -> AttributeValue(s = Some(id)), ":orderDate" -> AttributeValue(s = Some(DateCutOff))))
    )

}
