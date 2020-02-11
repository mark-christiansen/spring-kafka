package com.opi.kafka.streams;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class Config {
    @Bean
    @ConfigurationProperties(prefix = "kafka.streams")
    public Properties kafkaStreamProperties() {
        return new Properties();
    }
}