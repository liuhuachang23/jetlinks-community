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
import org.jetlinks.community.network.udp.UdpMessage;
import org.jetlinks.community.network.udp.UdpSupport;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.community.gateway.monitor.DeviceGatewayMonitor;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;

public class UdpDeviceSession implements DeviceSession {

    @Getter
    private final DeviceOperator operator;

    private UdpSupport support;

    @Setter
    private InetSocketAddress address;

    //记录最后一次心跳时间，初始化为当前时间。后面每次心跳都会更新
    private long lastPingTime = System.currentTimeMillis();

    //记录设备连接时间
    private final long connectTime = System.currentTimeMillis();

    //设备保持连接的超时时间。
    private long keepAliveTimeOutMS = Duration.ofMinutes(10).toMillis();

    private final DeviceGatewayMonitor monitor;

    @Getter
    private final Transport transport;

    public UdpDeviceSession(DeviceOperator operator,
                            UdpSupport support,
                            InetSocketAddress address,
                            Transport transport,
                            DeviceGatewayMonitor monitor) {
        this.operator = operator;
        this.support = support;
        this.address = address;
        this.transport = transport;
        this.monitor = monitor;
    }

    public void setSupport(UdpSupport support) {
        this.support = support;
      //  this.address = support.getBindAddress();
    }

    @Override
    public String getId() {
        return getDeviceId();
    }

    @Override
    public String getDeviceId() {
        return operator.getDeviceId();
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
        ping();
        return support
            .publish(new UdpMessage(encodedMessage.getPayload(), address))
            .doOnSuccess(v -> monitor.sentMessage())
            .thenReturn(true);
    }

    @Override
    public void close() {
        monitor.disconnected();
    }

    @Override
    public void ping() {
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        //keepAliveTimeOutMS 分钟之内还未收到设备的消息 就下线
        return keepAliveTimeOutMS < 0 || System.currentTimeMillis() - lastPingTime < keepAliveTimeOutMS;
    }

    @Override
    public void onClose(Runnable call) {

    }

    @Override
    public void setKeepAliveTimeout(Duration timeout) {
        keepAliveTimeOutMS = timeout.toMillis();
    }

    @Override
    public Optional<InetSocketAddress> getClientAddress() {
        return Optional.of(address);
    }
}
