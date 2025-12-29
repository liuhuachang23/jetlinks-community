package org.jetlinks.community.device.message.writer;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.Unpooled;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.KafkaClient;
import org.hswebframework.web.crud.events.EntityCreatedEvent;
import org.hswebframework.web.crud.events.EntityDeletedEvent;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.device.events.DeviceDeployedEvent;
import org.jetlinks.community.device.events.DeviceUnregisterEvent;
import org.jetlinks.community.messaging.kafka.KafkaProducer;
import org.jetlinks.community.messaging.kafka.ReactorKafkaProducer;
import org.jetlinks.community.messaging.kafka.SimpleMessage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 平台业务实体消息转发Kafka（未启用，启用）
 */
@Log4j2
@Component
@ConditionalOnClass(value = KafkaClient.class)
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(prefix = "device.entity.writer.kafka", name = "enabled", havingValue = "true")
public class KafkaDeviceEntitySubscriber {


    final KafkaProducer producer;

    final static String topic = "iot.device.entity";

    // 1. 定义事件类型Key常量，清晰区分不同操作
    private static final String KEY_CREATE = "DEVICE_CREATE";       // 创建设备
    private static final String KEY_SAVE = "DEVICE_SAVE";           // 保存（修改）设备
    private static final String KEY_DELETE = "DEVICE_DELETE";       // 删除设备
    private static final String KEY_ENABLE = "KEY_ENABLE";          // 启用（发布）设备
    private static final String KEY_DISABLE = "KEY_DISABLE";        // 禁用（注销）设备

    public KafkaDeviceEntitySubscriber(KafkaProperties properties,
                                       SslBundles sslBundles) {
        this.producer = new ReactorKafkaProducer(properties, sslBundles);
    }

    /**
     * 监听创建设备实体事件
     *
     * @param event
     */
    @EventListener
    public void handleDeviceEvent(EntityCreatedEvent<DeviceInstanceEntity> event) {
        log.debug("收到 EntityCreatedEvent:{}", event);
        event.async(
            sendToKafka(event.getEntity(), KEY_CREATE)
        );
    }


    /**
     * 监听保存设备实体事件（包含创建、修改）
     *
     * @param event
     */
    @EventListener
    public void handleDeviceEvent(EntitySavedEvent<DeviceInstanceEntity> event) {
        log.debug("收到 EntitySavedEvent:{}", event);
        event.async(
            sendToKafka(event.getEntity(), KEY_SAVE)
        );
    }

    /**
     * 监听删除设备实体事件
     *
     * @param event
     */
    @EventListener
    public void handleDeviceEvent(EntityDeletedEvent<DeviceInstanceEntity> event) {
        log.debug("收到 EntityDeletedEvent:{}", event);
        event.async(
            sendToKafka(event.getEntity(), KEY_DELETE)
        );
    }

    /**
     * 监听创建设备发布事件（即启用事件）
     *
     * @param event
     */
    @EventListener
    public void handleDeviceEvent(DeviceDeployedEvent event) {
        log.debug("收到 DeviceDeployedEvent:{}", event);
        event.async(
            sendToKafka(event.getDevices(), KEY_ENABLE)
        );
    }

    /**
     * 监听注销设备实体事件（即禁用事件）
     *
     * @param event
     */
    @EventListener
    public void handleDeviceEvent(DeviceUnregisterEvent event) {
        log.debug("收到 DeviceUnregisterEvent:{}", event);
        event.async(
            sendToKafka(event.getDevices(), KEY_DISABLE)
        );
    }


//    private Flux<DeviceInstanceEntity> sendToKafka(List<DeviceInstanceEntity> entity) {
//        return Flux.fromIterable(entity)
//                   .doOnNext(e -> producer.send(Mono.just(SimpleMessage.of(topic, e))));
//    }

    /**
     * 核心方法：发送消息到Kafka，Key为【事件类型】
     *
     * @param entities 设备实体列表
     * @param eventKey 事件类型Key
     * @return Flux<DeviceInstanceEntity> 处理完成的实体流
     */
    private Flux<DeviceInstanceEntity> sendToKafka(List<DeviceInstanceEntity> entities, String eventKey) {
        if (entities == null || entities.isEmpty()) {
            return Flux.empty(); // 空列表直接返回，避免无效操作
        }

        return Flux.fromIterable(entities)
                   .flatMap(entity ->
                                // 用 flatMap 等待发送完成
                                producer.send(Mono.just(SimpleMessage.of(
                                            topic,
                                            Unpooled.wrappedBuffer(eventKey.getBytes()),
                                            Unpooled.wrappedBuffer(JSON.toJSONBytes(entity))
                                        ))) // 建议添加 key（如实体ID）
                                        .doOnSuccess(v -> log.info("设备实体消息发送成功: {}", entity.getId()))
                                        .doOnError(e -> log.error("设备实体消息发送失败: {}", entity.getId(), e))
                                        .thenReturn(entity) // 发送完成后返回实体，保持 Flux 流
                   );
    }
}
