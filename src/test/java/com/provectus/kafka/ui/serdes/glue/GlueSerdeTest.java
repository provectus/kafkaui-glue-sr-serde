package com.provectus.kafka.ui.serdes.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.glue.model.DataFormat.AVRO;
import static software.amazon.awssdk.services.glue.model.DataFormat.JSON;
import static software.amazon.awssdk.services.glue.model.DataFormat.PROTOBUF;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.provectus.kafka.ui.serde.api.DeserializeResult;
import com.provectus.kafka.ui.serde.api.PropertyResolver;
import com.provectus.kafka.ui.serde.api.Serde;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.CreateRegistryRequest;
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.DeleteRegistryRequest;
import software.amazon.awssdk.services.glue.model.RegistryId;

class GlueSerdeTest {

  private static final Logger log = LoggerFactory.getLogger(GlueSerdeTest.class);

  private static final String REGION = System.getProperty(
      SdkSystemSetting.AWS_REGION.property(), Region.US_EAST_1.id());

  private static final String REGISTRY_NAME = "kui-glue-serde-test-registry";

  private static final KafkaContainer KAFKA = new KafkaContainer(
      DockerImageName.parse("confluentinc/cp-kafka:7.2.1")).withNetwork(Network.SHARED);

  private static GlueClient GLUE_CLIENT;

  @BeforeAll
  static void init() {
    checkCredsResolving();
    KAFKA.start();
    GLUE_CLIENT = GlueClient.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .httpClient(ApacheHttpClient.create())
        .region(Region.of(REGION))
        .build();
    try {
      GLUE_CLIENT.createRegistry(CreateRegistryRequest.builder().registryName(REGISTRY_NAME).build());
    } catch (AlreadyExistsException | AccessDeniedException e) {
      // already created / we cant create registries (but hoping it was crated beforehand)
    }
  }

  static void checkCredsResolving() {
    try (var provider = DefaultCredentialsProvider.create()) {
      provider.resolveCredentials();
    } catch (Exception e) {
      throw new IllegalStateException("Error resolving AWS credentials", e);
    }
  }

  @AfterAll
  static void tearDown() {
    KAFKA.close();
    try {
      GLUE_CLIENT.deleteRegistry(
          DeleteRegistryRequest.builder().registryId(
              RegistryId.builder().registryName(REGISTRY_NAME).build()).build());
    } catch (Exception e) {
      log.error("Error deletion test registry" + e);
    }
    GLUE_CLIENT.close();
  }

  private <T> KafkaProducer<String, T> createProducer(DataFormat dataFormat) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
    props.put(AWSSchemaRegistryConstants.AWS_REGION, REGION);
    props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, REGISTRY_NAME);
    props.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
    props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
    return new KafkaProducer<>(props);
  }

  private KafkaProducer<String, byte[]> createRawProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private <T> KafkaConsumer<String, T> createConsumer(DataFormat dataFormat) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup" + randomInt());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(AWSSchemaRegistryConstants.AWS_REGION, REGION);
    props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, REGISTRY_NAME);
    props.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
    props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
    props.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.DYNAMIC_MESSAGE.getName());
    return new KafkaConsumer<>(props);
  }

  private KafkaConsumer<Bytes, Bytes> createRawConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
    return new KafkaConsumer<>(props);
  }

  // Checks if Serde.Deserializer compatible with GlueSchemaRegistryKafkaSerializer
  private <T> void checkDeserializerIsCompatibleWithKafkaLibrarySerializer(
      DataFormat dataFormat,
      List<T> valuesToProduce,
      List<String> expectedDeserializedValues) throws Exception {
    String topic = "test-" + dataFormat.name().toLowerCase() + "-" + System.currentTimeMillis();
    try (KafkaProducer<String, T> producer = createProducer(dataFormat)) {
      // schema will be registered with topic name (by default) during producing
      valuesToProduce.forEach(v -> producer.send(new ProducerRecord<>(topic, 0, "key", v)));
    }
    List<ConsumerRecord<Bytes, Bytes>> polled = new ArrayList<>();
    try (KafkaConsumer<Bytes, Bytes> consumer = createRawConsumer()) {
      consumer.subscribe(List.of(topic));
      for (int i = 0; i < 5 && polled.size() < valuesToProduce.size(); i++) {
        consumer.poll(Duration.ofSeconds(1)).forEach(polled::add);
      }
    }
    Preconditions.checkArgument(polled.size() == valuesToProduce.size());
    try (GlueSerde serde = new GlueSerde()) {
      serde.configure(
          DefaultCredentialsProvider.create(),
          REGION,
          null,
          REGISTRY_NAME,
          null,
          "%s",
          List.of(),
          List.of(),
          false
      );
      assertTrue(serde.canDeserialize(topic, Serde.Target.VALUE));

      var deserializer = serde.deserializer(topic, Serde.Target.VALUE);
      for (int i = 0; i < expectedDeserializedValues.size(); i++) {
        var deserializeResult = deserializer.deserialize(null, polled.get(i).value().get());
        assertEquals(DeserializeResult.Type.JSON, deserializeResult.getType());
        assertTrue(deserializeResult.getAdditionalProperties().isEmpty());
        assertJsonEquals(expectedDeserializedValues.get(i), deserializeResult.getResult());
      }
    }
  }

  // Checks if Serde.Serializer compatible with GlueSchemaRegistryKafkaDeserializer
  private <T> void checkSerializerIsCompatibleWithKafkaLibraryDeserializer(
      DataFormat dataFormat,
      String schemaDefinition,
      List<String> jsonValuesToProduce,
      List<T> expectedDeserializedValues,
      Comparator<T> comparator) throws Exception {
    String topic = "test-" + dataFormat.name().toLowerCase() + "-" + System.currentTimeMillis();
    registerSchema(topic, dataFormat, schemaDefinition);
    try (GlueSerde serde = new GlueSerde();
         var producer = createRawProducer()) {
      serde.configure(
          DefaultCredentialsProvider.create(),
          REGION,
          null,
          REGISTRY_NAME,
          null,
          "%s",
          List.of(),
          List.of(),
          false
      );
      assertTrue(serde.canSerialize(topic, Serde.Target.VALUE));

      var serializer = serde.serializer(topic, Serde.Target.VALUE);
      for (int i = 0; i < expectedDeserializedValues.size(); i++) {
        String k = String.valueOf(i);
        byte[] val = serializer.serialize(jsonValuesToProduce.get(i));
        producer.send(new ProducerRecord<>(topic, 0, k, val)).get();
      }

      List<T> polled = new ArrayList<>();
      try (KafkaConsumer<String, T> consumer = createConsumer(dataFormat)) {
        consumer.subscribe(List.of(topic));
        for (int i = 0; i < 5 && polled.size() != expectedDeserializedValues.size(); i++) {
          for (var rec : consumer.poll(Duration.ofSeconds(1))) {
            polled.add(rec.value());
          }
        }
      }
      Assertions.assertEquals(expectedDeserializedValues.size(), polled.size());
      for (int i = 0; i < expectedDeserializedValues.size(); i++) {
        Assertions.assertEquals(0, comparator.compare(expectedDeserializedValues.get(i), polled.get(i)));
      }
    }
  }

  @Test
  void testAvroFormatSerdeCompatibility() throws Exception {
    var schema = new Schema.Parser().parse(
        "{"
            + "  \"type\": \"record\","
            + "  \"name\": \"TestAvroRecord1\","
            + "  \"fields\": ["
            + "    {"
            + "      \"name\": \"field1\","
            + "      \"type\": \"string\""
            + "    },"
            + "    {"
            + "      \"name\": \"field2\","
            + "      \"type\": \"int\""
            + "    }"
            + "  ]"
            + "}"
    );

    var v1 = new GenericRecordBuilder(schema)
        .set("field1", randomInt() + "")
        .set("field2", randomInt())
        .build();

    var v2 = new GenericRecordBuilder(schema)
        .set("field1", randomInt() + "")
        .set("field2", randomInt())
        .build();

    checkDeserializerIsCompatibleWithKafkaLibrarySerializer(
        AVRO,
        List.of(v1, v2),
        List.of(JsonUtil.avroRecordToJson(v1), JsonUtil.avroRecordToJson(v2))
    );

    checkSerializerIsCompatibleWithKafkaLibraryDeserializer(
        AVRO,
        schema.toString(),
        List.of(JsonUtil.avroRecordToJson(v1), JsonUtil.avroRecordToJson(v2)),
        List.of(v1, v2),
        Comparator.naturalOrder()
    );
  }

  @Test
  void testProtoFormatSerdeCompatibility() throws Exception {
    var schema = "syntax = \"proto3\";\n"
        + "package com.provectus;\n"
        + "\n"
        + "message TestProtoRecord {\n"
        + "  string field1 = 1;\n"
        + "  int32 field2 = 2;\n"
        + "}\n";

    Descriptors.Descriptor descriptor = new ProtobufSchema(schema).toDescriptor();

    var v1 = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("field1"), randomInt() + "")
        .setField(descriptor.findFieldByName("field2"), randomInt())
        .build();

    var v2 = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("field1"), randomInt() + "")
        .setField(descriptor.findFieldByName("field2"), randomInt())
        .build();

    checkDeserializerIsCompatibleWithKafkaLibrarySerializer(
        PROTOBUF,
        List.of(v1, v2),
        List.of(JsonUtil.protoMsgToJson(v1), JsonUtil.protoMsgToJson(v2))
    );

    checkSerializerIsCompatibleWithKafkaLibraryDeserializer(
        PROTOBUF,
        schema,
        List.of(JsonUtil.protoMsgToJson(v1), JsonUtil.protoMsgToJson(v2)),
        List.of(v1, v2),
        Comparator.comparing(DynamicMessage::toString)
    );
  }

  @Test
  void testJsonSchemaFormatSerdeCompatibility() throws Exception {
    String jsonSchema = "{ "
        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\", "
        + "  \"$id\": \"http://example.com/myURI.schema.json\", "
        + "  \"title\": \"TestRecord\","
        + "  \"type\": \"object\","
        + "  \"additionalProperties\": false,"
        + "  \"properties\": {"
        + "    \"f1\": {"
        + "      \"type\": \"integer\""
        + "    },"
        + "    \"f2\": {"
        + "      \"type\": \"string\""
        + "    }"
        + "  }"
        + "}";

    String v1 = new ObjectNode(JsonNodeFactory.instance)
        .put("f1", randomInt())
        .put("f2", randomInt() + "")
        .toString();

    String v2 = new ObjectNode(JsonNodeFactory.instance)
        .put("f1", randomInt())
        .put("f2", randomInt() + "")
        .toString();

    checkDeserializerIsCompatibleWithKafkaLibrarySerializer(
        JSON,
        List.of(JsonDataWithSchema.builder(jsonSchema, v1).build(), JsonDataWithSchema.builder(jsonSchema, v2).build()),
        List.of(v1, v2)
    );

    checkSerializerIsCompatibleWithKafkaLibraryDeserializer(
        JSON,
        jsonSchema,
        List.of(v1, v2),
        List.of(JsonDataWithSchema.builder(jsonSchema, v1).build(), JsonDataWithSchema.builder(jsonSchema, v2).build()),
        Comparator.comparing(JsonDataWithSchema::getPayload)
    );
  }

  private void assertJsonEquals(String expected, String actual) throws JsonProcessingException {
    var mapper = new JsonMapper();
    assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

  private static int randomInt() {
    return ThreadLocalRandom.current().nextInt(1000000);
  }

  @Test
  void canSerializeAndCanDeserializeCheckTopicSchemaMappingMap() {
    String testKeySchema = "testKeySchema-" + UUID.randomUUID();
    String testValueSchema = "testValSchema-" + UUID.randomUUID();

    try (GlueSerde serde = new GlueSerde()) {
      serde.configure(
          DefaultCredentialsProvider.create(),
          REGION,
          null,
          REGISTRY_NAME,
          null,
          "%s",
          List.of(Map.entry(testKeySchema, Pattern.compile("topic1|topic2"))),
          List.of(Map.entry(testValueSchema, Pattern.compile("topic3|topic4"))),
          true
      );

      Consumer<String> schemaCreator = name -> registerSchema(name, AVRO, "{\"type\":\"string\"}");
      schemaCreator.accept(testKeySchema);
      schemaCreator.accept(testValueSchema);

      assertTrue(serde.canDeserialize("topic1", Serde.Target.KEY));
      assertTrue(serde.canDeserialize("topic2", Serde.Target.KEY));
      assertFalse(serde.canDeserialize("topic3", Serde.Target.KEY));

      assertTrue(serde.canDeserialize("topic3", Serde.Target.VALUE));
      assertTrue(serde.canDeserialize("topic4", Serde.Target.VALUE));
      assertFalse(serde.canDeserialize("topic1", Serde.Target.VALUE));

      assertTrue(serde.canSerialize("topic1", Serde.Target.KEY));
      assertTrue(serde.canSerialize("topic2", Serde.Target.KEY));
      assertFalse(serde.canSerialize("topic3", Serde.Target.KEY));

      assertTrue(serde.canSerialize("topic3", Serde.Target.VALUE));
      assertTrue(serde.canSerialize("topic4", Serde.Target.VALUE));
      assertFalse(serde.canSerialize("topic1", Serde.Target.VALUE));
    }
  }

  private void registerSchema(String name, DataFormat dataFormat, String schemaDef) {
    GLUE_CLIENT.createSchema(
        CreateSchemaRequest.builder()
            .registryId(RegistryId.builder().registryName(REGISTRY_NAME).build())
            .schemaName(name)
            .dataFormat(dataFormat.name().toUpperCase())
            .compatibility(Compatibility.FULL)
            .schemaDefinition(schemaDef)
            .build()
    );
  }

  @Test
  void canSerializeAndCanDeserializeUsesTopicKVTemplateToFindSchemas() {
    String topicName = "testTopic-" + UUID.randomUUID();
    Consumer<String> schemaCreator = name -> registerSchema(name, AVRO, "{\"type\":\"string\"}");
    schemaCreator.accept(topicName + "-key");
    schemaCreator.accept(topicName + "-value");

    try (GlueSerde serde = new GlueSerde()) {
      serde.configure(
          DefaultCredentialsProvider.create(),
          REGION,
          null,
          REGISTRY_NAME,
          "%s-key",
          "%s-value",
          List.of(),
          List.of(),
          true
      );
      assertTrue(serde.canDeserialize(topicName, Serde.Target.KEY));
      assertTrue(serde.canDeserialize(topicName, Serde.Target.VALUE));
      assertFalse(serde.canDeserialize("some-other-topic", Serde.Target.KEY));
      assertFalse(serde.canDeserialize("some-other-topic", Serde.Target.VALUE));

      assertTrue(serde.canSerialize(topicName, Serde.Target.KEY));
      assertTrue(serde.canSerialize(topicName, Serde.Target.VALUE));
      assertFalse(serde.canSerialize("some-other-topic", Serde.Target.KEY));
      assertFalse(serde.canSerialize("some-other-topic", Serde.Target.VALUE));
    }
  }

  @Test
  void canDeserializeReturnsTrueForAnyTopicIfSchemaExistenceCheckIsDisabled() {
    Function<Boolean, GlueSerde> factory = checkEnabled -> {
      GlueSerde serde = new GlueSerde();
      serde.configure(
          DefaultCredentialsProvider.create(),
          REGION,
          null,
          REGISTRY_NAME,
          "%s-key",
          "%s-value",
          List.of(),
          List.of(),
          checkEnabled
      );
      return serde;
    };

    try (GlueSerde serdeWithDisabledCheck = factory.apply(false)) {
      assertTrue(serdeWithDisabledCheck.canDeserialize("anyTopic", Serde.Target.KEY));
      assertTrue(serdeWithDisabledCheck.canDeserialize("anyTopic", Serde.Target.VALUE));
    }

    try (GlueSerde serdeWithEnabledCheck = factory.apply(true)) {
      assertFalse(serdeWithEnabledCheck.canDeserialize("anyTopic", Serde.Target.KEY));
      assertFalse(serdeWithEnabledCheck.canDeserialize("anyTopic", Serde.Target.VALUE));
    }
  }

  @Test
  void serdePropertiesUsedAsAwsCredsIfSet() {
    var serdeProps = mock(PropertyResolver.class);
    when(serdeProps.getProperty("awsAccessKeyId", String.class))
        .thenReturn(Optional.of("awsAccessKeyId_test"));
    when(serdeProps.getProperty("awsSecretAccessKey", String.class))
        .thenReturn(Optional.of("awsSecretAccessKey_test"));

    var createdProvider = GlueSerde.createCredentialsProvider(serdeProps);
    var creds = createdProvider.resolveCredentials();
    assertEquals("awsAccessKeyId_test", creds.accessKeyId());
    assertEquals("awsSecretAccessKey_test", creds.secretAccessKey());
  }

  @Test
  void defaultCredsProviderUsedIfNoCredsPropertiesSet() {
    var serdeProps = mock(PropertyResolver.class);
    var createdProvider = GlueSerde.createCredentialsProvider(serdeProps);
    assertEquals(DefaultCredentialsProvider.create(), createdProvider);
  }

}