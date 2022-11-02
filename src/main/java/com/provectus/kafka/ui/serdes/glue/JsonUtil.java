package com.provectus.kafka.ui.serdes.glue;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

class JsonUtil {

  private JsonUtil() {
  }

  static String avroRecordToJson(GenericRecord record) {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      Schema schema = record.getSchema();
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
      DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
      writer.write(record, encoder);
      encoder.flush();
      return out.toString();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  static String protoMsgToJson(DynamicMessage msg) {
    try {
      return JsonFormat.printer()
          .includingDefaultValueFields()
          .omittingInsignificantWhitespace()
          .print(msg);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

}
