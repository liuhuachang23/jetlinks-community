package org.jetlinks.community.device.configuration;

import com.rabbitmq.client.ConnectionFactory;
import org.jetlinks.community.device.message.writer.RabbitMQMessageWriterConnector;
import org.jetlinks.community.device.service.data.DeviceDataService;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;

@Configuration
@ConditionalOnClass(ConnectionFactory.class)
@EnableConfigurationProperties(RabbitProperties.class)
@ConditionalOnProperty(prefix = "device.message.writer.rabbitmq", name = "enabled", havingValue = "true")
public class RabbitMQMessageWriterConnectorConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "device.message.writer.rabbitmq")
    public RabbitMQMessageWriterConnector rabbitMQMessageWriterConnector(DeviceDataService dataService,
                                                                         Scheduler scheduler,
                                                                         RabbitProperties rabbitProperties) {
        RabbitMQMessageWriterConnector writerConnector = new RabbitMQMessageWriterConnector(dataService, rabbitProperties);
        writerConnector.setScheduler(scheduler);
        return writerConnector;
    }

}