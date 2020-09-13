package dev.amcosta.ex1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class Pipe {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-plaintext-input");
        source.to("streams-pipe-output");

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        // Client

        final KafkaStreams streams = new KafkaStreams(topology, properties);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shudown-hook") {
            @Override
            public void run() {
                System.out.println("The thread was started");

                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("Start stream");
            streams.start();
            latch.await();
        } catch (Throwable exception) {
            System.exit(1);
        }

        System.exit(0);

    }
}
