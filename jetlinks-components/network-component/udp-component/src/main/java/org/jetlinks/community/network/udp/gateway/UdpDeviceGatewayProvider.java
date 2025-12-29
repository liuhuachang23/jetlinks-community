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
package org.jetlinks.community.network.udp.gateway;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.community.gateway.DeviceGateway;
import org.jetlinks.community.network.udp.UdpSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.community.gateway.supports.DeviceGatewayProperties;
import org.jetlinks.community.gateway.supports.DeviceGatewayProvider;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkManager;
import org.jetlinks.community.network.NetworkType;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.supports.server.DecodedClientMessageHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * UDP设备网关提供商，提供对UDP设备网关对支持
 *
 * @author zhouhao
 * @since 1.0
 */
@Component
public class UdpDeviceGatewayProvider implements DeviceGatewayProvider {

    private final NetworkManager networkManager;

    private final ProtocolSupports protocolSupports;

    private final DeviceSessionManager sessionManager;

    private final DecodedClientMessageHandler messageHandler;

    private final DeviceRegistry registry;

    public UdpDeviceGatewayProvider(NetworkManager networkManager, ProtocolSupports protocolSupports, DeviceSessionManager sessionManager, DecodedClientMessageHandler messageHandler, DeviceRegistry registry) {
        this.networkManager = networkManager;
        this.protocolSupports = protocolSupports;
        this.sessionManager = sessionManager;
        this.messageHandler = messageHandler;
        this.registry = registry;
    }

    @Override
    public String getId() {
        return "udp-device-gateway";
    }

    @Override
    public String getName() {
        return "UDP 接入";
    }

    @Override
    public Transport getTransport() {
        return DefaultTransport.UDP;
    }

    public NetworkType getNetworkType() {
        return DefaultNetworkType.UDP;
    }

    @Override
    public Mono<UdpDeviceGateway> createDeviceGateway(DeviceGatewayProperties properties) {
        return networkManager
            .<UdpSupport>getNetwork(getNetworkType(), properties.getChannelId())
            .map(udp -> {
                String protocol = properties.getProtocol();
                if (!StringUtils.hasText(protocol)) {
                    throw new IllegalArgumentException("protocol can not be null");
                }
                return new UdpDeviceGateway(properties.getId(),
                    () -> protocolSupports.getProtocol(protocol),
                    sessionManager,
                    udp,
                    messageHandler,
                    registry
                );
            });
    }

    @Override
    public Mono<? extends DeviceGateway> reloadDeviceGateway(DeviceGateway gateway,
                                                             DeviceGatewayProperties properties) {
        UdpDeviceGateway deviceGateway = ((UdpDeviceGateway) gateway);
        //网络组件发生变化
        if (!Objects.equals(deviceGateway.getId(), properties.getId())) {
            return gateway
                .shutdown()
                .then(this.createDeviceGateway(properties))
                .flatMap(newer -> newer.startup().thenReturn(newer));
        }
        //更新协议
        String protocol = properties.getProtocol();
        deviceGateway.protocolSupplier = () -> protocolSupports.getProtocol(protocol);

        return Mono.just(gateway);
    }
}
