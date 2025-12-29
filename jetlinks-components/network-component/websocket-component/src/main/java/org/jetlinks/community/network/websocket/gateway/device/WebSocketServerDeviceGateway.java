package org.jetlinks.community.network.websocket.gateway.device;

import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.logger.ReactiveLogger;
import org.jetlinks.community.gateway.AbstractDeviceGateway;
import org.jetlinks.community.gateway.DeviceGateway;
import org.jetlinks.community.gateway.monitor.DeviceGatewayMonitor;
import org.jetlinks.community.gateway.monitor.GatewayMonitors;
import org.jetlinks.community.gateway.monitor.MonitorSupportDeviceGateway;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkType;
import org.jetlinks.community.network.utils.DeviceGatewayHelper;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.community.network.websocket.server.WebSocketServer;
import org.jetlinks.community.network.websocket.server.WebSocketServerClient;
import org.jetlinks.supports.server.DecodedClientMessageHandler;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * WebSocket服务设备网关，使用网络组件中的WebSocket服务来处理设备数据
 *
 * @author zhouhao
 * @since 1.0
 */
@Slf4j
class WebSocketServerDeviceGateway extends AbstractDeviceGateway implements DeviceGateway, MonitorSupportDeviceGateway {

    private final DeviceRegistry registry;

    private final WebSocketServer webSocketServer;

    private final LongAdder counter = new LongAdder();

    private final Function<WebSocketServerClient, Mono<ProtocolSupport>> protocolSupplier;

    private final DeviceGatewayMonitor gatewayMonitor;

    private final EmitterProcessor<Message> messageProcessor = EmitterProcessor.create(false);

    private final FluxSink<Message> sink = messageProcessor.sink(FluxSink.OverflowStrategy.BUFFER);

    private final AtomicBoolean started = new AtomicBoolean();

    private Disposable disposable;

    private final DeviceGatewayHelper helper;

    WebSocketServerDeviceGateway(String id,
                                 DeviceRegistry registry,
                                 DeviceSessionManager sessionManager,
                                 WebSocketServer webSocketServer,
                                 DecodedClientMessageHandler messageHandler,
                                 Function<WebSocketServerClient, Mono<ProtocolSupport>> protocolSupplier) {
        super(id);
        this.registry = registry;
        this.webSocketServer = webSocketServer;
        this.gatewayMonitor = GatewayMonitors.getDeviceGatewayMonitor(id);
        this.protocolSupplier = protocolSupplier;
        this.helper = new DeviceGatewayHelper(registry, sessionManager, messageHandler);
    }


    public Transport getTransport() {
        return DefaultTransport.WebSocket;
    }

    public NetworkType getNetworkType() {
        return DefaultNetworkType.WEB_SOCKET_SERVER;
    }

    private void doStart() {
        if (started.getAndSet(true) || disposable != null) {
            return;
        }
        disposable = webSocketServer
            .handleConnection()
            .filter(conn -> {
                if (!started.get()) {
                    conn.close(WebSocketCloseStatus.BAD_GATEWAY.code());
                    gatewayMonitor.rejected();
                }
                return started.get();
            })
            .flatMap(client -> protocolSupplier.apply(client).zipWith(Mono.just(client)), Integer.MAX_VALUE)
            .flatMap(tp -> {
                WebSocketServerClient client = tp.getT2();
                WebSocketDeviceSession firstSession = new WebSocketDeviceSession(null, client
                    .getRemoteAddress()
                    .orElse(null), client, getTransport(), null);

                gatewayMonitor.connected();
                client.closeHandler(gatewayMonitor::disconnected);
                return tp.getT2()
                         .receive()
                         .filter(pb -> started.get())
                         .doOnNext(msg -> gatewayMonitor.receivedMessage())
                         .flatMap(socketMessage -> tp
                             .getT1()
                             .getMessageCodec(getTransport())
                             .flatMapMany(codec -> codec.decode(FromDeviceMessageContext.of(firstSession, socketMessage, registry)))
                             .cast(DeviceMessage.class)
                             .flatMap(deviceMessage -> {
                                 if (messageProcessor.hasDownstreams()) {
                                     sink.next(deviceMessage);
                                 }
                                 return helper
                                     .handleDeviceMessage(deviceMessage,
                                                          device -> {
                                                              if (firstSession.getOperator() == null) {
                                                                  firstSession.setOperator(device);
                                                              }
                                                              WebSocketDeviceSession deviceSession = firstSession.copy();
                                                              deviceSession.setOperator(device);
                                                              return deviceSession;
                                                          },
                                                          DeviceGatewayHelper
                                                              .applySessionKeepaliveTimeout(
                                                                  deviceMessage, firstSession::getKeepaliveTimeout
                                                              ),
                                                          () -> log.warn("无法从websocket请求[{}]消息中获取设备信息:{}", client
                                                              .getRemoteAddress()
                                                              .orElse(null), deviceMessage)
                                     );

                             })
                             .doOnEach(ReactiveLogger.onError(err -> log.error("处理WebSocket[{}]消息失败:{}", firstSession.getAddress(), socketMessage, err)))
                             .onErrorResume((err) -> Mono.empty()));
            }, Integer.MAX_VALUE)
            .contextWrite(ReactiveLogger.start("network", webSocketServer.getId()))
            .onErrorContinue((err, obj) -> log.error("处理WebSocket连接失败", err))
            .subscribe();
    }

    @Override
    public Flux<Message> onMessage() {
        return messageProcessor;
    }

    @Override
    public Mono<Void> doStartup() {
        return Mono.fromRunnable(this::doStart);
    }

    @Override
    public Mono<Void> doShutdown() {
        return Mono.fromRunnable(() -> {
            started.set(false);
            if (disposable != null && !disposable.isDisposed()) {
                disposable.dispose();
            }
            disposable = null;
        });
    }

    @Override
    public long totalConnection() {
        return counter.sum();
    }
}
