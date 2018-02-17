package de.codecentric.kafka.demo.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import de.codecentric.kafka.demo.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

@Configuration
public class AvroProducerConfig {
	
	@Value("${kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapServers;
	
	@Value("${kafka.schemaregistry-url:http://localhost:8081}")
	private String schemRegistryUrl;

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers,"No bootsrap servers"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemRegistryUrl);
		return props;
	}

	@Bean
	public ProducerFactory<String, User> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public KafkaTemplate<String, User> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public AvroProducer avroProducer() {
		return new AvroProducer();
	}

}
