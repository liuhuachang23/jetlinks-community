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
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.gateway.AbstractDeviceGateway;
import org.jetlinks.community.gateway.monitor.MonitorSupportDeviceGateway;
import org.jetlinks.community.network.udp.UdpMessage;
import org.jetlinks.community.network.udp.UdpSupport;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.community.gateway.DeviceGateway;
import org.jetlinks.community.gateway.monitor.DeviceGatewayMonitor;
import org.jetlinks.community.gateway.monitor.GatewayMonitors;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkType;
import org.jetlinks.community.network.utils.DeviceGatewayHelper;
import org.jetlinks.supports.server.DecodedClientMessageHandler;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * UDP设备网关，使用指定的协议包，将网络组件中UDP支持的请求处理为设备消息
 *
 * @author zhouhao
 * @since 1.0
 */
@Slf4j
public class UdpDeviceGateway extends AbstractDeviceGateway implements DeviceGateway, MonitorSupportDeviceGateway {

    Supplier<Mono<ProtocolSupport>> protocolSupplier;

    private final UdpSupport udpSupport;

    private final DeviceRegistry registry;

    private final LongAdder counter = new LongAdder();

    final AtomicReference<DeviceSession> sessionRef = new AtomicReference<>();

    private final EmitterProcessor<Message> processor = EmitterProcessor.create(false);

    private final FluxSink<Message> sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);

    private final DeviceGatewayMonitor monitor;

    private final DeviceGatewayHelper helper;

    public UdpDeviceGateway(String id,
                            Supplier<Mono<ProtocolSupport>> protocolSupplier,
                            DeviceSessionManager sessionManager,
                            UdpSupport udpSupport,
                            DecodedClientMessageHandler messageHandler,
                            DeviceRegistry registry) {

        super(id);
        this.protocolSupplier = protocolSupplier;
        this.udpSupport = udpSupport;
        this.registry = registry;
        this.monitor = GatewayMonitors.getDeviceGatewayMonitor(id, "network", "udp");
        this.helper = new DeviceGatewayHelper(registry, sessionManager, messageHandler);
    }

    public Transport getTransport() {
        return DefaultTransport.UDP;
    }

    public NetworkType getNetworkType() {
        return DefaultNetworkType.UDP;
    }

    @Override
    public Flux<Message> onMessage() {
        return processor;
    }

    private Disposable disposable;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private void doStart() {
        if (started.getAndSet(true) || disposable != null) {
            return;
        }
        disposable = udpSupport
            .subscribe()
            .filter(msg -> started.get())
            .flatMap(msg -> Mono.just(msg)
                                .zipWith(protocolSupplier
                                             .get()
                                             .onErrorResume(err -> Mono.fromRunnable(() -> log.warn("获取协议失败", err)))))
            .flatMap(tp2 -> {
                ProtocolSupport protocol = tp2.getT2();
                UdpMessage message = tp2.getT1();
                log.debug("收到UDP[{}]报文:\n{}", message.getAddress(), message);

                AtomicReference<Duration> keepAliveTimeoutRef = new AtomicReference<>();

                return protocol
                    .getMessageCodec(getTransport())
                    .flatMapMany(codec -> codec.decode(new FromDeviceMessageContext() {
                        @Override
                        public DeviceSession getSession() {
                            return new UnknownUdpDeviceSession(udpSupport, message.getAddress(), getTransport(), monitor) {
                                @Override
                                public void setKeepAliveTimeout(Duration timeout) {
                                    keepAliveTimeoutRef.set(timeout);
                                }
                            };
                        }

                        @Override
                        @Nonnull
                        public EncodedMessage getMessage() {
                            return message;
                        }

                        @Override
                        public Mono<DeviceOperator> getDevice(String deviceId) {
                            return registry.getDevice(deviceId);
                        }
                    }))
                    .doOnNext(msg -> monitor.receivedMessage())
                    .cast(DeviceMessage.class)
                    .flatMap(msg -> {
                        if (processor.hasDownstreams()) {
                            sink.next(msg);
                        }
                        return helper
                            .handleDeviceMessage(
                                msg,
                                device -> new UdpDeviceSession(device, udpSupport, message.getAddress(), getTransport(), monitor),
                                DeviceGatewayHelper
                                    .applySessionKeepaliveTimeout(msg, keepAliveTimeoutRef::get)
                                    .andThen(session -> {
                                        UdpDeviceSession deviceSession = session.unwrap(UdpDeviceSession.class);
                                        deviceSession.setAddress(message.getAddress());
                                        deviceSession.setSupport(udpSupport);
                                    }),
                                () -> log.warn("无法从udp请求[{}]消息中获取设备信息:{}", message.getAddress(), msg)
                            );
                    })
                    .onErrorResume((err) -> {
                        log.error("处理UDP[{}]消息失败", message, err);
                        return Mono.empty();
                    });
            })
            .onErrorContinue((err, v) -> log.error("处理UDP消息失败", err))
            .doFinally(s -> log.debug("upd device gateway closed "))
            .subscribe();

    }

    @Override
    protected Mono<Void> doShutdown() {
        return Mono.fromRunnable(() -> {
            started.set(false);
            if (disposable != null && !disposable.isDisposed()) {
                disposable.dispose();
            }
            disposable = null;
        });
    }

    @Override
    protected Mono<Void> doStartup() {
        return Mono.fromRunnable(this::doStart);
    }

    @Override
    public long totalConnection() {
        return counter.sum();
    }
}
