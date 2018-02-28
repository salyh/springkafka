package de.codecentric.kafka.demo.avro.streams;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfiguration {
	
	@Value("${kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapServers;
	
	@Value("${kafka.schemaregistry-url:http://localhost:8081}")
	private String schemRegistryUrl;
	
	@Value("${kakfa.topic.avro:avromessages}")
	private String avroTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers,"No bootsrap servers"));
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "userStreams");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemRegistryUrl);
        return new StreamsConfig(props);
    }

    
    @Bean
    public KStream<String, String> kStream(KStreamBuilder kStreamBuilder) {
        KStream<String, GenericRecord> stream1 = kStreamBuilder.stream("topic1");
        KStream<String, GenericRecord> stream2 = kStreamBuilder.stream("topic2");
        
        KStream<String, String> joined = stream1.join(stream2,
        		(leftValue, rightValue) -> leftValue.get("name")+"-"+rightValue.get("name"),
        		JoinWindows.of(TimeUnit.MINUTES.toMillis(5)));
        
        joined.to(Serdes.String(),Serdes.String(),"topic3");
        joined.print();
        return joined;
    }

}
