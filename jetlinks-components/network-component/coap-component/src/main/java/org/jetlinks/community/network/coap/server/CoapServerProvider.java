package org.jetlinks.community.network.coap.server;

import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.server.resources.Resource;
import org.eclipse.californium.elements.Connector;
import org.eclipse.californium.elements.UDPConnector;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.config.DtlsConfig;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;
import org.eclipse.californium.scandium.dtls.CertificateType;
import org.eclipse.californium.scandium.dtls.pskstore.AdvancedSinglePskStore;
import org.eclipse.californium.scandium.dtls.x509.NewAdvancedCertificateVerifier;
import org.eclipse.californium.scandium.dtls.x509.SingleCertificateProvider;
import org.eclipse.californium.scandium.dtls.x509.StaticNewAdvancedCertificateVerifier;
import org.hswebframework.web.bean.FastBeanCopier;
import org.jetlinks.community.network.*;
import org.jetlinks.core.metadata.ConfigMetadata;
import org.jetlinks.core.metadata.DefaultConfigMetadata;
import org.jetlinks.core.metadata.types.BooleanType;
import org.jetlinks.core.metadata.types.IntType;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.community.network.monitor.NetMonitors;
import org.jetlinks.community.network.security.Certificate;
import org.jetlinks.community.network.security.CertificateManager;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import org.eclipse.californium.elements.config.Configuration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.X509KeyManager;
import java.net.InetSocketAddress;
import java.security.PrivateKey;

/**
 * CoAP 服务网络组建提供商,提供在网络组建中的CoAP服务支持.
 *
 * @author zhouhao
 * @since 1.0
 */
@Component
public class CoapServerProvider implements NetworkProvider<CoapServerProperties> {

    private final CertificateManager certificateManager;

    public CoapServerProvider(CertificateManager certificateManager) {
        this.certificateManager = certificateManager;
    }

    @Nonnull
    @Override
    public NetworkType getType() {
        return DefaultNetworkType.COAP_SERVER;
    }

    @Nonnull
    @Override
    public Mono<Network> createNetwork(@Nonnull CoapServerProperties properties) {

        VertxCoapServer defaultCoapServer = new VertxCoapServer(
            properties.getId(),
            NetMonitors.getMonitor("coap-server", "id", properties.getId())
        );
       return initCoapServer(defaultCoapServer, properties);
    }

    private Mono<Network> initCoapServer(VertxCoapServer server, CoapServerProperties properties) {
        try {
            CoapServer coapServer = new CoapServer() {
                @Override
                protected Resource createRoot() {
                    return server;
                }
            };
            coapServer.addEndpoint(new CoapEndpoint.Builder()
                                       .setConnector(createConnector(properties))
                                       .build());
            server.setBindAddress(new InetSocketAddress(properties.getAddress(), properties.getPort()));
            server.setCoapServer(coapServer);
        } catch (Throwable e) {
            server.setLastError(e.getMessage());
            throw e;
        }
        return Mono.just(server);
    }

    private Connector createConnector(CoapServerProperties properties) {
        if (properties.isEnableDtls()) {
            Certificate certificate = properties.getCertificate();
            X509KeyManager x509KeyManager = certificate.getX509KeyManager(properties.getPrivateKeyAlias());
            if (x509KeyManager == null) {
                throw new IllegalArgumentException("key alias not found");
            }
            PrivateKey privateKey = x509KeyManager.getPrivateKey(null);
            if (privateKey == null) {
                throw new IllegalArgumentException("private key not found");
            }

            // 使用新的Configuration方式配置DTLS
            Configuration configuration = new Configuration();
            configuration.set(DtlsConfig.DTLS_ROLE, DtlsConfig.DtlsRole.CLIENT_ONLY);

            DtlsConnectorConfig.Builder builder = new DtlsConnectorConfig.Builder(configuration);

            // 设置PSK存储 - 使用新的AdvancedSinglePskStore
            AdvancedSinglePskStore pskStore = new AdvancedSinglePskStore("identity", "key".getBytes());
            builder.setAdvancedPskStore(pskStore);

            // 设置证书提供者
            SingleCertificateProvider certificateProvider = new SingleCertificateProvider(
                privateKey,
                x509KeyManager.getCertificateChain(null),
                CertificateType.RAW_PUBLIC_KEY,
                CertificateType.X_509
            );
            builder.setCertificateIdentityProvider(certificateProvider);

            // 设置证书验证器
            NewAdvancedCertificateVerifier certificateVerifier = StaticNewAdvancedCertificateVerifier.builder()
                                                                                                     .setTrustedCertificates(certificate.getTrustCerts())
                                                                                                     .build();
            builder.setAdvancedCertificateVerifier(certificateVerifier);

            return new DTLSConnector(builder.build());
        }

        // 对于UDP连接，使用新的构造函数
        return new UDPConnector(properties.createSocketAddress(), Configuration.getStandard());
    }



    @Override
    public Mono<Network> reload(@Nonnull Network network, @Nonnull CoapServerProperties properties) {
        return initCoapServer(((VertxCoapServer) network), properties);
    }

    @Nullable
    @Override
    public ConfigMetadata getConfigMetadata() {

        return new DefaultConfigMetadata()
            .add("id", "id", "", new StringType())
            .add("address", "服务地址", "", new StringType())
            .add("certId", "证书id", "", new StringType())
            .add("enableDtls", "是否开启dtls", "", new BooleanType())
            .add("port", "服务端口", "", new IntType())
            .add("privateKeyAlias", "私钥别名", "", new StringType());
    }

    @Nonnull
    @Override
    public Mono<CoapServerProperties> createConfig(@Nonnull NetworkProperties properties) {
        return Mono.defer(() -> {
            CoapServerProperties clientProperties = FastBeanCopier.copy(properties.getConfigurations(), new CoapServerProperties());
            clientProperties.setId(properties.getId());
            clientProperties.validate();
            if (clientProperties.isEnableDtls()) {
                return certificateManager.getCertificate(clientProperties.getCertId())
                                         .switchIfEmpty(Mono.error(() -> new UnsupportedOperationException("证书[" + clientProperties
                                             .getCertId() + "]不存在")))
                                         .doOnNext(clientProperties::setCertificate)
                                         .thenReturn(clientProperties);
            }

            return Mono.just(clientProperties);
        });
    }

}
