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
import org.jetlinks.community.network.udp.UdpMessage;
import org.jetlinks.community.network.udp.UdpSupport;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.community.gateway.monitor.DeviceGatewayMonitor;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

public class UnknownUdpDeviceSession implements DeviceSession {


    private UdpSupport support;

    private InetSocketAddress address;

    private long lastPingTime;

    private long connectTime = System.currentTimeMillis();

    private DeviceGatewayMonitor monitor;

    public UnknownUdpDeviceSession(UdpSupport support,
                                   InetSocketAddress address,
                                   Transport transport, DeviceGatewayMonitor monitor) {
        this.support = support;
        this.address = address;
        this.transport = transport;
        this.monitor=monitor;
    }

    @Getter
    private Transport transport;

    @Override
    public DeviceOperator getOperator() {
        return null;
    }

    @Override
    public String getId() {
        return "udp-" + address.toString();
    }

    @Override
    public String getDeviceId() {
        return "known";
    }


    @Override
    public long lastPingTime() {
        return lastPingTime;
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public Mono<Boolean> send(EncodedMessage encodedMessage) {
        return support
            .publish(new UdpMessage(encodedMessage.getPayload(), address))
            .doOnSuccess((r)->monitor.sentMessage())
            .thenReturn(true);
    }

    @Override
    public void close() {

    }

    @Override
    public void ping() {
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        return true;
    }

    @Override
    public void onClose(Runnable call) {

    }
}
