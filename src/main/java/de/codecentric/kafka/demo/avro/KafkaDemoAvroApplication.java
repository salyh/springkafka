package de.codecentric.kafka.demo.avro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaDemoAvroApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoAvroApplication.class, args);
	}
}
