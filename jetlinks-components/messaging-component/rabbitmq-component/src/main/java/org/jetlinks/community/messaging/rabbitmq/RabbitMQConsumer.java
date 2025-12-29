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
package org.jetlinks.community.messaging.rabbitmq;

import reactor.core.publisher.Flux;

public interface RabbitMQConsumer {

    /**
     * 订阅消息,多次订阅会收到相同的消息.
     *
     * @return 消息流
     */
    Flux<AmqpMessage> subscribe();

    /**
     * 停止消费
     */
    void shutdown();

}
