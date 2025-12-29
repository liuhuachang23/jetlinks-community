package org.jetlinks.community.device.configuration;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.KafkaClient;
import org.jetlinks.community.device.message.writer.KafkaMessageWriterConnector;
import org.jetlinks.community.device.service.data.DeviceDataService;
import org.jetlinks.core.event.EventBus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
@ConditionalOnClass(value = {KafkaClient.class, EventBus.class})
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(prefix = "device.message.writer.kafka", name = "enabled", havingValue = "true")
public class KafkaMessageWriterConnectorConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "device.message.writer.kafka")
    public KafkaMessageWriterConnector kafkaMessageWriterConnector(DeviceDataService dataService,
                                                                   KafkaProperties kafkaProperties) {
        return new KafkaMessageWriterConnector(dataService, kafkaProperties, null);
    }

}