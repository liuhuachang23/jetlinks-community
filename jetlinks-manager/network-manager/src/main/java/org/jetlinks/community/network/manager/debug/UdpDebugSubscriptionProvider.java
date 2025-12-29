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
package org.jetlinks.community.network.manager.debug;

import org.jetlinks.community.gateway.external.SubscribeRequest;
import org.jetlinks.community.gateway.external.SubscriptionProvider;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkManager;
import org.jetlinks.community.network.udp.UdpMessage;
import org.jetlinks.community.network.udp.UdpSupport;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
public class UdpDebugSubscriptionProvider implements SubscriptionProvider {

    private final NetworkManager networkManager;

    public UdpDebugSubscriptionProvider(NetworkManager networkManager) {
        this.networkManager = networkManager;
    }

    @Override
    public String id() {
        return "network-udp-debug";
    }

    @Override
    public String name() {
        return "UDP调试";
    }

    @Override
    public String[] getTopicPattern() {
        return new String[]{
            "/network/udp/*/_send",
            "/network/udp/*/_subscribe"
        };
    }

    @Override
    public Flux<String> subscribe(SubscribeRequest request) {
        String id = request.getTopic().split("[/]")[3];

        if (request.getTopic().endsWith("_send")) {
            return send(id, request);
        } else {
            return subscribe(id);
        }
    }

    public Flux<String> send(String id, SubscribeRequest request) {
        UdpMessage message = request.getString("request")
            .map(UdpMessage::of)
            .orElseThrow(() -> new IllegalArgumentException("参数[request]不能为空"));

        return networkManager
            .<UdpSupport>getNetwork(DefaultNetworkType.UDP, id)
            .flatMap(client -> client.publish(message))
            .thenReturn("推送成功")
            .flux();
    }

    public Flux<String> subscribe(String id) {

        return networkManager
            .<UdpSupport>getNetwork(DefaultNetworkType.UDP, id)
            .flatMapMany(server -> server
                .subscribe()
                .map(UdpMessage::toString));
    }


}
