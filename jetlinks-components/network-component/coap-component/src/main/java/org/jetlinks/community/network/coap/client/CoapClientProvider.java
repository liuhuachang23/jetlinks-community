package org.jetlinks.community.network.coap.client;

import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.elements.Connector;
import org.eclipse.californium.elements.UDPConnector;
import org.eclipse.californium.elements.config.Configuration;
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
import org.jetlinks.core.metadata.types.LongType;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.community.network.security.Certificate;
import org.jetlinks.community.network.security.CertificateManager;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.X509KeyManager;
import java.net.InetSocketAddress;
import java.security.PrivateKey;

/**
 * CoAP Client网络组建提供商,用于对CoAP Client提供支持
 *
 * @author zhouhao
 * @since 1.0
 */
@Component
public class CoapClientProvider implements NetworkProvider<CoapClientProperties> {

    private final CertificateManager certificateManager;

    public CoapClientProvider(CertificateManager certificateManager) {
        this.certificateManager = certificateManager;
    }

    @Nonnull
    @Override
    public NetworkType getType() {
        return DefaultNetworkType.COAP_CLIENT;
    }

    @Nonnull
    @Override
    public Mono<Network> createNetwork(@Nonnull CoapClientProperties properties) {
        VertxCoapClient coapClient = new VertxCoapClient(createCoapClient(properties), properties);
        //return initCoapClient(coapClient, properties);
        return Mono.just(coapClient);
    }

    private Mono<Network> initCoapClient(VertxCoapClient client, CoapClientProperties properties) {
        try {
            // 根据属性配置重新创建客户端
            org.eclipse.californium.core.CoapClient coapClient = createCoapClient(properties);
            client.client = coapClient;
            client.properties = properties;
            return Mono.just(client);
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    public Mono<Network> reload(@Nonnull Network network, @Nonnull CoapClientProperties properties) {
        VertxCoapClient coapClient = ((VertxCoapClient) network);
        coapClient.client = createCoapClient(properties);
        coapClient.properties = properties;
        //return initCoapClient(coapClient, properties);
        return Mono.just(coapClient);
    }

    private org.eclipse.californium.core.CoapClient createCoapClient(CoapClientProperties properties) {
        org.eclipse.californium.core.CoapClient coapClient = new CoapClient();
        coapClient.setEndpoint(new CoapEndpoint.Builder()
                                   .setConnector(createConnector(properties))
                                   .build());

        return coapClient;
    }

    private Connector createConnector(CoapClientProperties properties) {
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
        return new UDPConnector(new InetSocketAddress(0), Configuration.getStandard());
    }

    @Nullable
    @Override
    public ConfigMetadata getConfigMetadata() {
        return new DefaultConfigMetadata()
            .add("id", "id", "", new StringType())
            .add("url", "请求服务路径", "", new StringType())
            .add("certId", "证书id", "", new StringType())
            .add("enableDtls", "是否开启dtls", "", new BooleanType())
            .add("timeout", "请求超时时间", "", new LongType())
            .add("retryTimes", "重试次数", "", new IntType())
            .add("privateKeyAlias", "私钥别名", "", new StringType());
    }

    @Nonnull
    @Override
    public Mono<CoapClientProperties> createConfig(@Nonnull NetworkProperties properties) {

        return Mono.defer(() -> {
            CoapClientProperties clientProperties = FastBeanCopier.copy(properties.getConfigurations(), new CoapClientProperties());
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
