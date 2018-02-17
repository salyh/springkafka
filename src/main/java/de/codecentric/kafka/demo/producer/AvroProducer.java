package de.codecentric.kafka.demo.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import de.codecentric.kafka.demo.avro.User;

public class AvroProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroProducer.class);

	@Value("${kakfa.topic.avro:avromessages}")
	private String avroTopic;

	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	public void send(User user) {
		LOGGER.info("sending user='{}'", user.toString());
		kafkaTemplate.send(avroTopic, user);
	}
}
