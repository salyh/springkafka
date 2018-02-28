package de.codecentric.kafka.demo.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFutureCallback;

import de.codecentric.kafka.demo.avro.producer.AvroProducer;

//@SpringBootApplication
@EnableScheduling
public class KafkaProducingDemoAvroApplication {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducingDemoAvroApplication.class);
	
	@Autowired
	private AvroProducer producer;
	
	@Scheduled(fixedRate = 1000)
	public void produce() {
		producer.send(new User("User "+System.currentTimeMillis(),1,"blue"))
		   .addCallback(new ListenableFutureCallback<SendResult<String, User>>() {

			@Override
			public void onSuccess(SendResult<String, User> result) {
				LOGGER.info("Sent "+result.getProducerRecord().value()+" to partition "+result.getRecordMetadata().partition()+ " with offset "+result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				LOGGER.error(ex+" sending user", ex);
			}
		});
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducingDemoAvroApplication.class, args);
	}
}
