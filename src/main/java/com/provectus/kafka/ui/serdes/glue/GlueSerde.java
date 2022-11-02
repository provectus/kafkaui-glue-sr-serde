package com.provectus.kafka.ui.serdes.glue;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.DynamicMessage;
import com.provectus.kafka.ui.serde.api.DeserializeResult;
import com.provectus.kafka.ui.serde.api.RecordHeaders;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import com.provectus.kafka.ui.serde.api.PropertyResolver;
import com.provectus.kafka.ui.serde.api.SchemaDescription;
import com.provectus.kafka.ui.serde.api.Serde;
import java.util.Optional;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.SchemaId;

public class GlueSerde implements Serde {

  private final LoadingCache<String, Boolean> schemaExistenceCache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(CacheLoader.from(this::schemaExists));

  private GlueClient glueClient;
  private GlueSchemaRegistryDeserializationFacade deserializationFacade;

  private String registryName;

  @Nullable
  private String keySchemaNameTemplate;
  private String valueSchemaNameTemplate;

  // schema name -> topics patterns
  private List<Map.Entry<String, Pattern>> topicKeysSchemas;
  private List<Map.Entry<String, Pattern>> topicValuesSchemas;

  @Override
  public void configure(PropertyResolver serdeProperties,
                        PropertyResolver clusterProperties,
                        PropertyResolver appProperties) {
    configure(
        createCredentialsProvider(),
        serdeProperties.getProperty("region", String.class)
            .orElseThrow(() -> new IllegalArgumentException("region not provided for GlueSerde")),
        serdeProperties.getProperty("endpoint", String.class).orElse(null),
        serdeProperties.getProperty("registry", String.class)
            .orElseThrow(() -> new IllegalArgumentException("registry not provided for GlueSerde")),
        serdeProperties.getProperty("keySchemaNameTemplate", String.class)
            .orElse(null), // there is no default for that
        serdeProperties.getProperty("valueSchemaNameTemplate", String.class)
            .orElse("%s"), // by default Serializer supposes that schemaName == topic name
        serdeProperties.getMapProperty("topicKeysSchemas", String.class, String.class)
            .orElse(Map.of())
            .entrySet()
            .stream()
            .map(e -> Map.entry(e.getKey(), Pattern.compile(e.getValue())))
            .collect(Collectors.toUnmodifiableList()),
        serdeProperties.getMapProperty("topicValuesSchemas", String.class, String.class)
            .orElse(Map.of())
            .entrySet()
            .stream()
            .map(e -> Map.entry(e.getKey(), Pattern.compile(e.getValue())))
            .collect(Collectors.toUnmodifiableList())
    );
  }

  void configure(AwsCredentialsProvider credentialsProvider,
                 String region,
                 @Nullable String endpoint,
                 String registryName,
                 @Nullable String keySchemaNameTemplate,
                 String valueSchemaNameTemplate,
                 List<Map.Entry<String, Pattern>> topicKeysSchemas,
                 List<Map.Entry<String, Pattern>> topicValuesSchemas) {
    this.glueClient = GlueClient.builder()
        .region(Region.of(region))
        .endpointOverride(Optional.ofNullable(endpoint).map(URI::create).orElse(null))
        .credentialsProvider(credentialsProvider)
        .httpClient(ApacheHttpClient.create())
        .build();
    this.deserializationFacade = new GlueSchemaRegistryDeserializationFacade(glueSrConfig(region, endpoint), credentialsProvider);
    this.registryName = registryName;
    this.keySchemaNameTemplate = keySchemaNameTemplate;
    this.valueSchemaNameTemplate = valueSchemaNameTemplate;
    this.topicKeysSchemas = topicKeysSchemas;
    this.topicValuesSchemas = topicValuesSchemas;
  }

  private AwsCredentialsProvider createCredentialsProvider() {
    // maybe provide tuning options in the future
    return DefaultCredentialsProvider.create();
  }

  private static GlueSchemaRegistryConfiguration glueSrConfig(String region, @Nullable String endpoint) {
    GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(region);
    config.setProtobufMessageType(ProtobufMessageType.DYNAMIC_MESSAGE);
    config.setAvroRecordType(AvroRecordType.GENERIC_RECORD);
    config.setSchemaAutoRegistrationEnabled(false);
    config.setEndPoint(endpoint);
    return config;
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.empty();
  }

  @Override
  public Optional<SchemaDescription> getSchema(String topic, Target target) {
    return Optional.empty();
  }

  @Override
  public boolean canSerialize(String topic, Target target) {
    return false;
  }

  @Override
  public Serializer serializer(String topic, Target target) {
    throw new IllegalStateException("Serialization not implemented yet");
  }

  @Override
  public boolean canDeserialize(String topic, Target target) {
    switch (target) {
      case KEY:
        return findSchemaByPattern(topicKeysSchemas, topic).isPresent()
            || keySchemaNameTemplate != null && schemaExistsCached(String.format(keySchemaNameTemplate, topic));
      case VALUE:
        return findSchemaByPattern(topicValuesSchemas, topic).isPresent()
            || schemaExistsCached(String.format(valueSchemaNameTemplate, topic));
      default:
        return false;
    }
  }

  private Optional<String> findSchemaByPattern(List<Map.Entry<String, Pattern>> patterns, String topicName) {
    return patterns.stream()
        .filter(e -> e.getValue().matcher(topicName).matches())
        .map(Map.Entry::getKey)
        .findFirst();
  }

  private boolean schemaExistsCached(String schemaName) {
    try {
      return schemaExistenceCache.get(schemaName);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean schemaExists(String schemaName) {
    try {
      glueClient.getSchema(
          GetSchemaRequest.builder()
              .schemaId(
                  SchemaId.builder()
                      .registryName(registryName)
                      .schemaName(schemaName).build())
              .build());
      return true;
    } catch (EntityNotFoundException nfe){
      return false;
    }
  }

  @Override
  public Deserializer deserializer(String topic, Target target) {
    return new Deserializer() {
      @Override
      public DeserializeResult deserialize(RecordHeaders recordHeaders, byte[] bytes) {
        Object obj = deserializationFacade.deserialize(AWSDeserializerInput.builder().buffer(ByteBuffer.wrap(bytes)).build());
        String val = null;
        if (obj instanceof GenericRecord) {
          val = JsonUtil.avroRecordToJson((GenericRecord) obj);
        } else if (obj instanceof DynamicMessage) {
          val = JsonUtil.protoMsgToJson((DynamicMessage) obj);
        } else if (obj instanceof JsonDataWithSchema) {
          val = ((JsonDataWithSchema) obj).getPayload();
        } else {
          throw new IllegalStateException("Unexpected deserialization result: " + obj);
        }
        return new DeserializeResult(val, DeserializeResult.Type.JSON, Map.of());
      }
    };
  }

  @Override
  public void close() {
    glueClient.close();
    deserializationFacade.close();
  }
}
