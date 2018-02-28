package de.codecentric.kafka.demo.avro.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

@Configuration
@EnableKafka
public class AvroConsumerConfig {

	@Value("${kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapServers;
	
	@Value("${kafka.schemaregistry-url:http://localhost:8081}")
	private String schemRegistryUrl;

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "avrotest");
		//props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 11000);
		props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemRegistryUrl);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		return props;
	}

	@Bean
	public ConsumerFactory<String, ConsumerRecord<String, GenericRecord>> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, ConsumerRecord<String, GenericRecord>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, ConsumerRecord<String, GenericRecord>> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.getContainerProperties().setAckOnError(false);
	    factory.getContainerProperties().setErrorHandler(new ErrorHandler() {
			
			@Override
			public void handle(Exception thrownException, ConsumerRecord<?, ?> data) {
				System.err.println(thrownException+" for "+data);
				thrownException.printStackTrace(System.err);
			}
		});
	    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
		return factory;
	}

	@Bean
	public AvroConsumer avroConsumer() {
		return new AvroConsumer();
	}

}
