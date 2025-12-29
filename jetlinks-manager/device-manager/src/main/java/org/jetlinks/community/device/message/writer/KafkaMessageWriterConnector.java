/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.community.device.message.writer;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.gateway.annotation.Subscribe;
import org.jetlinks.community.device.service.data.DeviceDataService;
import org.jetlinks.community.gateway.DeviceMessageUtils;
import org.jetlinks.community.messaging.kafka.ReactorKafkaConsumer;
import org.jetlinks.community.messaging.kafka.ReactorKafkaProducer;
import org.jetlinks.community.messaging.kafka.SimpleMessage;
import org.jetlinks.community.messaging.rabbitmq.SimpleAmqpMessage;
import org.jetlinks.community.rule.engine.entity.AlarmHistoryInfo;
import org.jetlinks.community.utils.MessageTypeMatcher;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.message.DeviceMessage;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;

/**
 *  Kafka消息写入器，将设备消息写入Kafka主题。
 */
@Slf4j
public class KafkaMessageWriterConnector implements CommandLineRunner {

    private final ReactorKafkaProducer producer;

    private final ReactorKafkaConsumer kafkaConsumer;

    private final DeviceDataService dataService;

    @Getter
    @Setter
    private String topicName = "device.message";

    @Getter
    @Setter
    private String deviceMessageTopicName = "iot.device.message";

    @Getter
    @Setter
    private String alarmRecordTopicName =  "iot.alarm.record";

    @Getter
    @Setter
    private String alarmLogTopicName =  "iot.alarm.log";


    @Getter
    @Setter
    private boolean consumer = true;


    @Getter
    @Setter
    private MessageTypeMatcher type = new MessageTypeMatcher();

    public KafkaMessageWriterConnector(DeviceDataService dataService,
                                       KafkaProperties properties, SslBundles sslBundles) {
        this.producer = new ReactorKafkaProducer(properties,sslBundles);
        this.kafkaConsumer = new ReactorKafkaConsumer(Collections.singleton(topicName), properties,sslBundles);
        this.dataService = dataService;
    }

    /**
     * 注解式订阅平台设备消息
     */
    @Subscribe(topics = "/device/**", id = "device-message-kafka-writer")
    public Mono<Void> writeDeviceMessageToMq(TopicPayload payload) {

        ByteBuf topic = Unpooled.wrappedBuffer(payload.getTopic().getBytes());
        DeviceMessage message = payload.decode(DeviceMessage.class);
        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(message.toJson()));
        if (!type.match(message.getMessageType())) {
            return Mono.empty();
        }

        return producer
                .send(Mono.just(SimpleMessage.of(deviceMessageTopicName, topic, messageBuf)))
                .subscribeOn(Schedulers.boundedElastic());
    }


    /**
     * 注解式订阅平台告警记录
     */
    @Subscribe(topics = "/alarm/*/*/*/record", id = "alarm-record-kafka-writer")
    public Mono<Void> writeAlarmRecordToMq(AlarmHistoryInfo alarmHistoryInfo) {
        ByteBuf topicBuf = Unpooled.wrappedBuffer(alarmHistoryInfo.getTargetType().getBytes());
        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(alarmHistoryInfo));
        return producer
                .send(Mono.just(SimpleMessage.of(alarmRecordTopicName, topicBuf, messageBuf)))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 注解式订阅平台告警日志
     */
    @Subscribe(topics = "/alarm/*/*/*/*/log", id = "alarm-log-kafka-writer")
    public Mono<Void> writeAlarmLogToMq(TopicPayload payload) {
        ByteBuf topic = Unpooled.wrappedBuffer(payload.getTopic().getBytes());
        AlarmHistoryInfo message = payload.decode(AlarmHistoryInfo.class);
        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(message));
        return producer
            .send(Mono.just(SimpleMessage.of(alarmLogTopicName, topic, messageBuf)))
            .subscribeOn(Schedulers.boundedElastic());
    }



    private final Disposable.Composite disposable = Disposables.composite();
    @PreDestroy
    public void shutdown() {
        this.producer.shutdown();
        if (this.kafkaConsumer != null) {
            this.kafkaConsumer.shutdown();
        }
        disposable.dispose();
    }

    @Override
    public void run(String... args) {
        if (!consumer) {
            return;
        }
        kafkaConsumer
                .subscribe()
                .flatMap(msg -> Mono.justOrEmpty(DeviceMessageUtils.convert(msg.getPayload())))
                .bufferTimeout(3000, Duration.ofSeconds(3))
                .publishOn(Schedulers.parallel())
                .flatMap(list -> dataService
                        .saveDeviceMessage(list)
                        .then(Mono.fromRunnable(() -> {
                            log.debug("write device message [{}] success", list.size());
                        }))
                        .retryWhen(Retry.fixedDelay(10, Duration.ofSeconds(2)))
                        .onErrorResume((err) -> {
                            log.error("write device data error", err);
                            return Mono.empty();
                        })
                )
                .subscribe();
    }
}