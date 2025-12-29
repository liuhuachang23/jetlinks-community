package org.jetlinks.community.network.websocket.gateway.device;

import com.alibaba.fastjson.JSONArray;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.gateway.supports.DeviceGatewayProperties;
import org.jetlinks.community.gateway.supports.DeviceGatewayProvider;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkManager;
import org.jetlinks.community.network.NetworkType;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.topic.Router;
import org.jetlinks.community.network.websocket.server.WebSocketServer;
import org.jetlinks.supports.server.DecodedClientMessageHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * WebSocket服务设备网关提供商
 *
 * @author zhouhao
 * @see WebSocketServerDeviceGateway
 * @since 1.0
 */
@Component
@AllArgsConstructor
@Slf4j
public class WebSocketServerDeviceGatewayProvider implements DeviceGatewayProvider {

    private final DeviceRegistry registry;

    private final DeviceSessionManager sessionManager;

    private final NetworkManager networkManager;

    private final DecodedClientMessageHandler messageHandler;

    private final ProtocolSupports protocolSupports;

    @Override
    public String getId() {
        return "websocket-server";
    }

    @Override
    public String getName() {
        return "WebSocket服务接入";
    }

    public NetworkType getNetworkType() {
        return DefaultNetworkType.WEB_SOCKET_SERVER;
    }

    public Transport getTransport() {
        return DefaultTransport.WebSocket;
    }

    @Override
    public Mono<WebSocketServerDeviceGateway> createDeviceGateway(DeviceGatewayProperties properties) {

        String id = properties.getId();

        return networkManager.<WebSocketServer>getNetwork(getNetworkType(), properties.getChannelId())
                .map(server -> {
                    //根据url路由获取协议包
                    Router<String, ProtocolSupport> router = Router.create();
                    List<RouteConfig> configs;
                    try {
                        configs = properties.get("routes")
                                .map(obj -> (List<Object>) obj)
                                .map(JSONArray::new)
                                .map(array -> array.toJavaList(RouteConfig.class))
                                .orElseThrow(() -> new NullPointerException());
                    } catch (Exception e) {
                        throw new IllegalArgumentException("参数routes错误", e);
                    }
                    for (RouteConfig config : configs) {
                        String url = config.getUrl();
                        if (StringUtils.isEmpty(url)) {
                            url = "/**";
                        }
                        router.route(url, ignore -> protocolSupports.getProtocol(config.protocol));
                    }
                    return new WebSocketServerDeviceGateway(
                            id, registry, sessionManager, server, messageHandler, client -> {
                        //根据url获取协议包
                        return router.execute(client.getUri(), client.getUri())
                                .take(1)
                                .singleOrEmpty()
                                .flatMap(Mono::from)
                                .doOnNext(protocol -> log.debug("use protocol [{}] for websocket request [{}]", protocol
                                        .getId(), client.getUri()))
                                .switchIfEmpty(Mono.fromRunnable(() -> log.warn("not found any protocol for websocket request [{}]", client
                                        .getUri())))
                                ;
                    });
                });
    }

//    @Override
//    public Mono<DeviceGateway> reloadDeviceGateway(DeviceGateway gateway, DeviceGatewayProperties properties) {
//        WebSocketServerDeviceGateway deviceGateway = ((WebSocketServerDeviceGateway) gateway);
//        //网络组件发生变化
//        if (!Objects.equals(deviceGateway.tcpServer.getId(), properties.getChannelId())) {
//            return gateway
//                .shutdown()
//                .then(this.createDeviceGateway(properties))
//                .flatMap(newer -> newer.startup().thenReturn(newer));
//        }
//        //更新协议
//        String protocol = properties.getProtocol();
//        deviceGateway.protocolSupplier = Mono.defer(() -> protocolSupports.getProtocol(protocol));
//
//        return Mono.just(gateway);
//    }

    @Getter
    @Setter
    public static class RouteConfig {
        private String url;

        private String protocol;
    }
}
