package com.example.taskmaster;

import com.example.taskmaster.config.TaskmasterProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(TaskmasterProperties.class)
public class TaskmasterApplication {

    public static void main(String[] args) {
        SpringApplication.run(TaskmasterApplication.class, args);
    }
}
