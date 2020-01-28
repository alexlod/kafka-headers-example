package io.confluent.ps.examples;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HeaderKafkaStreams {
  private static final Logger log = LoggerFactory.getLogger(HeaderKafkaStreams.class);

  public static final String OUTPUT_TOPIC = "test-out";

  private static Properties createConfig() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "header-kafka-streams");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    return props;
  }

  public static void main(final String[] args) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> messages = builder.stream(HeaderProducer.INPUT_TOPIC);
    messages.transform(LogTransformer::new);
    messages.to(OUTPUT_TOPIC);

    KafkaStreams app = new KafkaStreams(builder.build(), createConfig());

    app.cleanUp();
    app.start();

    Runtime.getRuntime().addShutdownHook(new Thread(app::close));
  }

  private static class LogTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
      this.context = processorContext;
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
      Header header = this.context.headers().lastHeader(LatencyHeader.HEADER_KEY);
      long originTimestamp = LatencyHeader.byteArrayToLong(header.value());
      log.info("Intermediate latency: " + (System.currentTimeMillis() - originTimestamp));

      return KeyValue.pair(key, value);
    }

    @Override
    public void close() {

    }
  }
}
