# zio-dynamodb-example

An repo containing example code of for using ZIO interop for Java Futures for working with the AWS java DynamoDB SDK.
It shows how to return a purely functional streaming interface (ZIO Streams) using server side paging.

All code is in the test [LocalDynamoDbSpec.scala](src/test/scala/dynamodb/LocalDynamoDbSpec.scala) which runs against
an in memory local dynamoDB instance.

To run:

    sbt test    
