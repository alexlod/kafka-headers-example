package io.confluent.ps.examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HeaderProducer {
  private static final Logger log = LoggerFactory.getLogger(HeaderProducer.class);

  public static final int MESSAGE_INTERVAL_MS = 1000;

  public static final String INPUT_TOPIC = "test";

  private static Properties createConfig() {
    Properties props = new Properties();

    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);

    return props;
  }

  public static void main(String[] args) {
    Producer<String, String> producer = new KafkaProducer<>(createConfig());

    while (true) {
      long now = System.currentTimeMillis();

      List<Header> headers = new ArrayList<>(1);
      headers.add(new LatencyHeader(now));

      ProducerRecord<String, String> record = new ProducerRecord<>(
              INPUT_TOPIC,
              null,
              now,
              "key",
              "value",
              headers);
      producer.send(record);
      log.info("Produced a message with timestamp " + now);
      try {
        Thread.sleep(MESSAGE_INTERVAL_MS);
      } catch (InterruptedException e) {
      }
    }
  }
}
