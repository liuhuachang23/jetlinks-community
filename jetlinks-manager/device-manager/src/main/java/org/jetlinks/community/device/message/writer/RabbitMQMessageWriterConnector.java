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
import com.rabbitmq.client.ConnectionFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.gateway.annotation.Subscribe;
import org.jetlinks.community.device.service.data.DeviceDataService;
import org.jetlinks.community.gateway.DeviceMessageUtils;
import org.jetlinks.community.messaging.kafka.SimpleMessage;
import org.jetlinks.community.messaging.rabbitmq.*;
import org.jetlinks.community.rule.engine.entity.AlarmHistoryInfo;
import org.jetlinks.community.utils.MessageTypeMatcher;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.message.DeviceMessage;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;

/**
 * RabbitMQ消息写入器，将设备消息写入RabbitMQ主题。
 */
@Slf4j
public class RabbitMQMessageWriterConnector implements CommandLineRunner {

    private RabbitMQProducer producer;

    private RabbitMQConsumer mqConsumer;

    private final DeviceDataService dataService;

    @Getter
    @Setter
    private String topicName = "iot.device.message";

    @Getter
    @Setter
    private String alarmTopicName =  "iot.alarm.record";

    @Getter
    @Setter
    private String alarmLogTopicName =  "iot.alarm.log";

    @Setter
    @Getter
    private String consumerRouteKey = "";

    @Setter
    @Getter
    private String producerRouteKey = "";

    @Getter
    @Setter
    private boolean consumer = true;

    @Getter
    @Setter
    private String group = "default";

    @Getter
    @Setter
    private boolean autoAck = true;

    @Getter
    @Setter
    private int threadSize = 4;

    private final RabbitProperties properties;

    @Getter
    @Setter
    private MessageTypeMatcher type = new MessageTypeMatcher();

    @Setter
    @Getter
    private Scheduler scheduler = Schedulers.parallel();

    public RabbitMQMessageWriterConnector(DeviceDataService dataService,
                                          RabbitProperties properties) {
        this.properties = properties;
        this.dataService = dataService;
    }

    /**
     * 注解式订阅平台设备消息
     * @param payload
     * @return
     */
    @Subscribe(topics = "/device/**", id = "device-message-rabbitmq-writer")
    public Mono<Void> writeDeviceMessageToMq(TopicPayload payload) {

        DeviceMessage message = payload.decode(DeviceMessage.class);

        if (!type.match(message.getMessageType())) {
            payload.release();
            return Mono.empty();
        }
        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(message.toJson()));
        return producer
                .publish(SimpleAmqpMessage.of(topicName, producerRouteKey, null, messageBuf))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 注解式订阅平台告警信息
     */
    @Subscribe(topics = "/alarm/*/*/*/record", id = "alarm-record-rabbitmq-writer")
    public Mono<Void> writeAlarmRecordToMq(AlarmHistoryInfo alarmHistoryInfo) {
        ByteBuf topicBuf = Unpooled.wrappedBuffer(alarmHistoryInfo.getTargetType().getBytes());
        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(alarmHistoryInfo));
        return producer
                .publish(SimpleAmqpMessage.of(alarmTopicName, producerRouteKey, null, messageBuf))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Subscribe(topics = "/alarm/*/*/*/*/log", id = "alarm-log-rabbitmq-writer")
    public Mono<Void> writeAlarmLogToMq(TopicPayload payload) {
        AlarmHistoryInfo message = payload.decode(AlarmHistoryInfo.class);
        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(message));
        return producer
            .publish(SimpleAmqpMessage.of(alarmTopicName, producerRouteKey, null, messageBuf))
            .subscribeOn(Schedulers.boundedElastic());
    }

    @PostConstruct
    public void init() {
        ConnectionFactory connectionFactory = RabbitUtils.createConnectionFactory(properties);
        connectionFactory.setAutomaticRecoveryEnabled(true);

        this.producer = new ReactorRabbitMQProducer(connectionFactory).init();
        if (consumer) {
            this.mqConsumer = new ReactorRabbitMQConsumer(topicName, true, connectionFactory)
                    .consumerGroup(group)
                    .consumerRouteKey(consumerRouteKey)
                    .consumerThread(threadSize)
                    .autoAck(autoAck)
                    .init();
        }
    }

    private final Disposable.Composite disposable = Disposables.composite();

    @PreDestroy
    public void shutdown() {
        this.producer.shutdown();
        if (this.mqConsumer != null) {
            this.mqConsumer.shutdown();
        }
        disposable.dispose();
    }

    /**
     * 运行消息消费任务
     *
     * @param args 启动参数数组
     */
    @Override
    public void run(String... args) {
        // 检查是否为消费者模式且消费者对象不为空
        if (!consumer || this.mqConsumer == null) {
            return;
        }

        disposable
                .add(mqConsumer
                        .subscribe()
                        // 缓冲消息：每3000条或每3秒批量处理一次，缓冲区初始容量为3000
                        .bufferTimeout(3000, Duration.ofSeconds(3), () -> new ArrayList<>(3000))
                        .flatMap(list -> Flux
                                .fromIterable(list)
                                // 转换消息负载为设备消息
                                .flatMap(ms -> Mono.justOrEmpty(DeviceMessageUtils.convert(ms.getPayload())))
                                // 保存设备消息到数据服务
                                .as(dataService::saveDeviceMessage)
                                // 失败时重试：固定延迟重试3次，每次间隔2秒
                                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2)))
                                .thenReturn(true)
                                // 异常处理：记录错误日志并返回false
                                .onErrorResume(err -> {
                                    log.error("write device data error", err);
                                    return Mono.just(false);
                                })
                                // 处理成功后的回调：记录日志并确认消息
                                .doOnSuccess((r) -> {
                                    if (r) {
                                        log.debug("write device message [{}] success", list.size());
                                    }
                                    // 遍历消息列表，根据处理结果进行确认或否定确认
                                    for (AmqpMessage amqpMessage : list) {
                                        if (!(amqpMessage instanceof AcknowledgableMessage)) {
                                            return;
                                        }
                                        if (r) {
                                            ((AcknowledgableMessage) amqpMessage).ack();
                                        } else {
                                            ((AcknowledgableMessage) amqpMessage).nack(true);
                                        }
                                    }
                                }), Integer.MAX_VALUE
                        )
                        // 取消订阅时的处理：重新启动消费任务
                        .doOnCancel(() -> {
                            //断掉了,重新消费.
                            if (!disposable.isDisposed()) {
                                run(args);
                            }
                        })
                        .subscribe()
                );
    }

}