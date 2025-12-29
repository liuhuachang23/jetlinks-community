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
package org.jetlinks.community.network.udp;

import org.jetlinks.community.network.ServerNetwork;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * UDP支持
 *
 * @author zhouhao
 */
public interface UdpSupport extends ServerNetwork {

    /**
     * 发送UDP消息
     *
     * @param message UDP消息
     * @return void
     */
    Mono<Void> publish(UdpMessage message);

    /**
     * 订阅UDP消息
     *
     * @return UDP消息流
     */
    Flux<UdpMessage> subscribe();

}
