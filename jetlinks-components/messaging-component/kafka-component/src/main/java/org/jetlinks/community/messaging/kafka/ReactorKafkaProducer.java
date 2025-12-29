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
package org.jetlinks.community.messaging.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.reactivestreams.Publisher;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class ReactorKafkaProducer implements KafkaProducer {

    private final KafkaProperties properties;
    private final SslBundles sslBundles; // Add SslBundles dependency

    public ReactorKafkaProducer(KafkaProperties properties, SslBundles sslBundles) {
        Objects.requireNonNull(properties);
        Objects.requireNonNull(properties.getProducer());
        this.properties = properties;
        this.sslBundles = sslBundles; // Initialize SslBundles
        init();
    }

    private KafkaSender<ByteBuffer, ByteBuffer> sender;

    private void init() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, properties.getClientId());

        props.putAll(properties.getProducer().buildProperties(sslBundles));
        props.putAll(properties.getProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);

        SenderOptions<ByteBuffer, ByteBuffer> senderOptions = SenderOptions.create(props);

        sender = KafkaSender.create(senderOptions);
    }

    @Override
    public Mono<Void> send(Publisher<Message> publisher) {
        if (sender == null) {
            return Mono.error(new IllegalStateException("kafka sender is shutdown"));
        }
        return sender
            .send(Flux
                .from(publisher)
                .map(msg -> SenderRecord
                    .create(new ProducerRecord<>(msg.getTopic(), msg.keyToNio(), msg.payloadToNio()), msg))
            )
            .flatMap(result -> {
                if (null != result.exception()) {
                    return Mono.error(result.exception());
                }
                if (log.isDebugEnabled()) {
                    RecordMetadata metadata = result.recordMetadata();
                    log.debug("Kafka Message {} sent successfully, topic-partition={}-{} offset={} timestamp={}",
                        result.correlationMetadata(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp());
                }
                return Mono.empty();
            })
            .then();
    }

    @Override
    public void shutdown() {
        if (null != sender) {
            sender.close();
        }
        sender = null;
    }
}
