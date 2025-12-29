package org.jetlinks.community.network.coap.gateway.device;

import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.Transport;

import org.jetlinks.community.gateway.DeviceGateway;
import org.jetlinks.community.gateway.supports.DeviceGatewayProperties;
import org.jetlinks.community.gateway.supports.DeviceGatewayProvider;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.NetworkManager;
import org.jetlinks.community.network.NetworkType;
import org.jetlinks.community.network.coap.server.CoapServer;
import org.jetlinks.supports.server.DecodedClientMessageHandler;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * CoAP 服务网关提供商,提供创建CoAP服务设备接入的能力
 *
 * @author zhouhao
 * @since 1.0
 */
@Component
public class CoAPServerDeviceGatewayProvider implements DeviceGatewayProvider {

    private final NetworkManager networkManager;

    private final DeviceRegistry registry;

    private final DecodedClientMessageHandler messageHandler;

    private final DeviceSessionManager sessionManager;

    private final ProtocolSupports protocolSupports;

    public CoAPServerDeviceGatewayProvider(NetworkManager networkManager,
                                           DeviceRegistry registry,
                                           DecodedClientMessageHandler messageHandler,
                                           DeviceSessionManager sessionManager,
                                           ProtocolSupports protocolSupports) {
        this.networkManager = networkManager;
        this.registry = registry;
        this.messageHandler = messageHandler;
        this.sessionManager = sessionManager;
        this.protocolSupports = protocolSupports;
    }

    @Override
    public String getId() {
        return "coap-server-gateway";
    }

    @Override
    public String getName() {
        return "CoAP 接入";
    }

    public NetworkType getNetworkType() {
        return DefaultNetworkType.COAP_SERVER;
    }

    public Transport getTransport() {
        return DefaultTransport.CoAP;
    }

    @Override
    public Mono<DeviceGateway> createDeviceGateway(DeviceGatewayProperties properties) {
        return networkManager
            .<CoapServer>getNetwork(getNetworkType(), properties.getChannelId())
            .map(server -> {

                String protocol = properties.getProtocol();

                return new CoAPServerDeviceGateway(properties.getId(),
                                                   registry,
                                                   server,
                                                   () -> protocolSupports.getProtocol(protocol),
                                                   sessionManager,
                                                   messageHandler);

            });
    }

    @Override
    public Mono<DeviceGateway> reloadDeviceGateway(DeviceGateway gateway, DeviceGatewayProperties properties) {

        CoAPServerDeviceGateway deviceGateway = ((CoAPServerDeviceGateway) gateway);
        //网络组件发生变化
        if (!Objects.equals(deviceGateway.getId(), properties.getId())) {
            return gateway
                .shutdown()
                .then(this.createDeviceGateway(properties))
                .flatMap(newer -> newer.startup().thenReturn(newer));
        }
        //更新协议
        String protocol = properties.getProtocol();
        deviceGateway.protocol = () -> protocolSupports.getProtocol(protocol);

        return Mono.just(gateway);
    }
}
