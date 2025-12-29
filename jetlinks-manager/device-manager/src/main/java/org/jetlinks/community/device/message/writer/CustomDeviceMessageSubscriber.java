///*
// * Copyright 2025 JetLinks https://www.jetlinks.cn
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.jetlinks.community.device.message.writer;
//
//import com.alibaba.fastjson.JSON;
//import io.netty.buffer.ByteBuf;
//import io.netty.buffer.Unpooled;
//import lombok.AllArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.jetlinks.community.messaging.kafka.KafkaProducer;
//import org.jetlinks.community.messaging.kafka.SimpleMessage;
//import org.jetlinks.community.rule.engine.entity.AlarmHistoryInfo;
//import org.jetlinks.core.event.EventBus;
//import org.jetlinks.core.event.Subscription;
//import org.jetlinks.core.event.TopicPayload;
//import org.jetlinks.core.message.DeviceMessage;
//import org.springframework.boot.CommandLineRunner;
//import reactor.core.publisher.Mono;
//
//
//@Slf4j
//@AllArgsConstructor
//public class CustomDeviceMessageSubscriber implements CommandLineRunner {
//
//    private final KafkaProducer producer;
//
//    private final EventBus eventBus;
//
//    private static String topicName = "custom-device.message";
//
//    private static String alramTopicName = "custom-device.alarm";
//
//    /**
//     * 编程式订阅，配合实现CommandLineRunner，在平台启动时调用该方法启动订阅线程
//     */
//    public void programmaticSubscription() {
//        eventBus.subscribe(Subscription
//                               .builder()
//                               .subscriberId("custom-device-subscriber")
//                               .topics("/device/**")
//                               .features(Subscription.Feature.local, Subscription.Feature.broker)
//                               .build(),
//                           (payload -> {
//                               if (log.isDebugEnabled()) {
//                                    log.debug("编程式订阅消息");
//                                }
//                               return kafkaPush(payload);
//                           }));
//
//    }
//
//        /**
//     * 编程式订阅，配合实现CommandLineRunner，在平台启动时调用该方法启动订阅线程
//     */
//    public void alarmProgrammaticSubscription() {
//        eventBus.subscribe(Subscription
//                               .builder()
//                               .subscriberId("custom-device-subscriber")
//                               .topics("/alarm/*/*/*/record")
//                               .features(Subscription.Feature.local, Subscription.Feature.broker)
//                               .build(),
//                           (alarmHistoryInfo -> {
//                               if (log.isDebugEnabled()) {
//                                    log.debug("编程式订阅消息");
//                                }
//                               return kafkaPush(alarmHistoryInfo);
//                           }));
//
//    }
//
//
//    private Mono<Void> kafkaPush(TopicPayload payload) {
//        DeviceMessage deviceMessage = (DeviceMessage) payload.decode();
//        ByteBuf topic = Unpooled.wrappedBuffer(payload.getTopic().getBytes());
//        DeviceMessage message = payload.decode(DeviceMessage.class);
//        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(message.toJson()));
//        //消息推送kafka
//		return producer.send(Mono.just(SimpleMessage.of(topicName, topic, messageBuf)));
//    }
//
//    public Mono<Void> kafkaPush(AlarmHistoryInfo alarmHistoryInfo) {
//        ByteBuf topicBuf = Unpooled.wrappedBuffer(alarmHistoryInfo.getTargetType().getBytes());
//        ByteBuf messageBuf = Unpooled.wrappedBuffer(JSON.toJSONBytes(alarmHistoryInfo));
//		return producer.send(Mono.just(SimpleMessage.of(alramTopicName, topicBuf, messageBuf)));
//    }
//
//
//    @Override
//    public void run(String... args) throws Exception {
//        programmaticSubscription();
//        alarmProgrammaticSubscription();
//    }
//
//}
