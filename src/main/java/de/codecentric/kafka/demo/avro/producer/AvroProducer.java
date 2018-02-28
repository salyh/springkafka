package de.codecentric.kafka.demo.avro.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import de.codecentric.kafka.demo.avro.User;

public class AvroProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroProducer.class);

	@Value("${kakfa.topic.avro:avromessages}")
	private String avroTopic;

	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	public ListenableFuture<SendResult<String, User>> send(User user) {
		LOGGER.info("sending user='{}'", user.toString());
		return kafkaTemplate.send(avroTopic, user);
	}
	
	public ListenableFuture<SendResult<String, User>> send(String key, User user, String topic) {
		LOGGER.info("sending user='{}' with key={} to {}", user.toString(), key, topic);
		return kafkaTemplate.send(topic, key, user);
	}
}
