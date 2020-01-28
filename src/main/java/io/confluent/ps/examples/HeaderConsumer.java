package io.confluent.ps.examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class HeaderConsumer {
  private static final Logger log = LoggerFactory.getLogger(HeaderConsumer.class);

  private static Properties createConfig() {
    Properties props = new Properties();

    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "header-consumer-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return props;
  }

  public static void main(String[] args) {
    Consumer<String, String> consumer = new KafkaConsumer<>(createConfig());
    consumer.subscribe(Collections.singletonList(HeaderKafkaStreams.OUTPUT_TOPIC));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
      for (ConsumerRecord<String, String> record : records) {
        Header header = record.headers().lastHeader(LatencyHeader.HEADER_KEY);
        long originTimestamp = LatencyHeader.byteArrayToLong(header.value());
        log.info("End-to-end latency: " + (System.currentTimeMillis() - originTimestamp));
      }
    }
  }
}
