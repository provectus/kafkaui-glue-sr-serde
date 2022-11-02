# Glue Schema Registry serde for kafka-ui

This is pluggable serde implementation for [kafka-ui](https://github.com/provectus/kafka-ui/).

You can read about Glue Schema [registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html) and how it can be applied for [Kafka usage](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

Currently, this serde only supports Deserialization and can be used for view messages in [kafka-ui](https://github.com/provectus/kafka-ui/).

For properties description and configuration example please see [docker-compose file](docker-compose/setup-example.yaml).

### Building locally

We use `DefaultCredentialsProvider` in tests, so should can configure env as described in its [documentation](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html). Authorized user should be able to create and delete Glue Schema registries and schemas.

Example:
```
mvn clean test -Daws.accessKeyId="..." -Daws.secretAccessKey="..."
```
