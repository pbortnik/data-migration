package com.epam.reportportal.servicecleaner;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(exclude = { org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration.class })
@EnableBatchProcessing
public class ServiceCleanerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ServiceCleanerApplication.class, args);
	}
}
