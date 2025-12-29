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

import io.netty.buffer.Unpooled;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.elements.*;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkType;
import org.jetlinks.community.network.monitor.NetMonitor;
import org.jetlinks.community.network.monitor.NetMonitors;
import org.jetlinks.rule.engine.executor.PayloadType;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

@Slf4j
public class DefaultUdpSupport implements UdpSupport {

    private Connector connector;

    private InetSocketAddress address;

    private final EmitterProcessor<UdpMessage> processor = EmitterProcessor.create(false);

    private final FluxSink<UdpMessage> sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
    @Getter
    private final String id;

    private final NetMonitor netMonitor ;

    @Getter
    @Setter
    private String lastError;

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private InetSocketAddress bindAddress;

    public DefaultUdpSupport(String id) {
        this.id = id;
        this.netMonitor = NetMonitors.getMonitor("udp-supports", "id", id);
    }

    @Override
    public NetworkType getType() {
        return DefaultNetworkType.UDP;
    }

    @Override
    public void shutdown() {
        if (null != connector) {
            connector.destroy();
            connector = null;
        }
    }

    @Override
    public boolean isAlive() {
        return connector != null;
    }

    @Override
    public boolean isAutoReload() {
        return false;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    @SneakyThrows
    public void setConnector(Connector connector) {
        this.connector = connector;
        this.connector.setRawDataReceiver(raw -> {
            byte[] payload = raw.getBytes();
            log.debug("Received UDP data from {}: {} bytes", raw.getInetSocketAddress(), payload.length); // 添加日志
            netMonitor.bytesRead(payload.length);
            if (processor.hasDownstreams()) {
                sink.next(new UdpMessage(Unpooled.wrappedBuffer(payload), raw.getInetSocketAddress()));
            }
        });
        this.connector.start();
    }

    @Override
    public Mono<Void> publish(UdpMessage message) {
        netMonitor.send();
        return Mono.<Void>create(sink -> {
            InetSocketAddress socketAddress = message.getAddress();
            if (socketAddress == null) {
                socketAddress = address;
            }
            if (socketAddress == null) {
                sink.error(new IllegalArgumentException("address cannot be null"));
                return;
            }
            if (connector == null) {
                sink.error(new UnsupportedOperationException("client Initializing"));
                return;
            }
            byte[] bytes = (byte[]) PayloadType.BINARY.read(message.getPayload());

            connector.send(RawData.outbound(bytes, new UdpEndpointContext(socketAddress), new MessageCallback() {
                @Override
                public void onConnecting() {

                }

                @Override
                public void onDtlsRetransmission(int flight) {

                }

                @Override
                public void onContextEstablished(EndpointContext context) {

                }

                @Override
                public void onSent() {
                    netMonitor.bytesSent(bytes.length);
                    sink.success();
                }

                @Override
                public void onError(Throwable error) {
                    sink.error(error);
                }
            }, false));
        }).doOnError(netMonitor::sendError);
    }

    @Override
    public Flux<UdpMessage> subscribe() {
        return processor;
    }
}
