package com.epam.reportportal.servicecleaner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class ServiceCleanerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ServiceCleanerApplication.class, args);
	}
}
