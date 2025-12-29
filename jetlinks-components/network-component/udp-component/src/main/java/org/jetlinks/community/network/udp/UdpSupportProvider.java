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

import org.eclipse.californium.elements.Connector;
import org.eclipse.californium.elements.UDPConnector;
import org.eclipse.californium.elements.config.Configuration;
import org.eclipse.californium.elements.config.UdpConfig;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.config.DtlsConfig;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;
import org.eclipse.californium.scandium.dtls.CertificateType;
import org.eclipse.californium.scandium.dtls.pskstore.AdvancedSinglePskStore;
import org.eclipse.californium.scandium.dtls.x509.StaticNewAdvancedCertificateVerifier;
import org.hswebframework.web.bean.FastBeanCopier;
import org.jetlinks.community.network.*;
import org.jetlinks.core.metadata.ConfigMetadata;
import org.jetlinks.core.metadata.DefaultConfigMetadata;
import org.jetlinks.core.metadata.types.BooleanType;
import org.jetlinks.core.metadata.types.IntType;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.community.network.security.Certificate;
import org.jetlinks.community.network.security.CertificateManager;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.X509KeyManager;
import java.security.PrivateKey;
import java.util.Arrays;

/**
 * UDP支持提供商
 *
 * @author zhouhao
 * @since 1.0
 */
@Component
@ConfigurationProperties(prefix = "jetlinks.network.udp-server")
public class UdpSupportProvider implements NetworkProvider<UdpSupportProperties> {

    private final CertificateManager certificateManager;

    public UdpSupportProvider(CertificateManager certificateManager) {
        this.certificateManager = certificateManager;
    }

    private Connector createConnector(UdpSupportProperties properties) {
        // 创建 Configuration 对象 TODO
        Configuration configuration = Configuration.createWithFile(
                Configuration.DEFAULT_FILE,         // 配置文件路径
                "UDP Configuration Header",           // 配置文件头信息
                null                                  // 自定义配置提供者（可以为 null）
        );

        // 设置 UDPConnector 的配置参数
        configuration.set(UdpConfig.UDP_CONNECTOR_OUT_CAPACITY, 1000); // 设置输出队列容量
        configuration.set(UdpConfig.UDP_RECEIVER_THREAD_COUNT, 2);     // 设置接收线程数
        configuration.set(UdpConfig.UDP_SENDER_THREAD_COUNT, 2);       // 设置发送线程数
        configuration.set(UdpConfig.UDP_DATAGRAM_SIZE, properties.getReceiverPacketSize()); // 设置数据包大小
        configuration.set(UdpConfig.UDP_RECEIVE_BUFFER_SIZE, 65535);  // 设置接收缓冲区大小
        configuration.set(UdpConfig.UDP_SEND_BUFFER_SIZE, 65535);      // 设置发送缓冲区大小

        if (properties.isDtls()) {
            Certificate certificate = properties.getCertificate();
            X509KeyManager x509KeyManager = certificate.getX509KeyManager(properties.getPrivateKeyAlias());
            if (x509KeyManager == null) {
                throw new IllegalArgumentException("key alias not found");
            }
            PrivateKey privateKey = x509KeyManager.getPrivateKey(null);
            if (privateKey == null) {
                throw new IllegalArgumentException("private key not found");
            }

            // 创建 AdvancedSinglePskStore
            AdvancedSinglePskStore pskStore = new AdvancedSinglePskStore(
                    "default-psk-identity", // 替换为实际的 PSK 身份
                    "default-psk-key".getBytes() // 替换为实际的 PSK 密钥
            );

            // 创建 StaticNewAdvancedCertificateVerifier
            StaticNewAdvancedCertificateVerifier certificateVerifier = new StaticNewAdvancedCertificateVerifier(
                    certificate.getTrustCerts(),
                    null, // 如果不使用 RPK，可以传入 null
                    Arrays.asList(CertificateType.X_509, CertificateType.RAW_PUBLIC_KEY)
            );

            // 创建 DtlsConnectorConfig.Builder 并传入 Configuration
            DtlsConnectorConfig.Builder dtlsConfigBuilder = new DtlsConnectorConfig.Builder(configuration)
                    .setAddress(properties.createLocalAddress())
                    .setAdvancedPskStore(pskStore)
                    .setAdvancedCertificateVerifier(certificateVerifier);

//            // 设置其他可选属性（根据需求）
//            dtlsConfigBuilder
//                .setUseReuseAddress(true) // 是否启用地址重用
//                .setProtocolVersionForHelloVerifyRequests(ProtocolVersion.VERSION_DTLS_1_2) // 设置 DTLS 协议版本
//                .setCipherSuiteSelector(CipherSuiteSelector.fromString("DEFAULT")) // 设置加密套件
//                .setSupportedGroups(Arrays.asList(XECDHECryptography.SupportedGroup.SECP256R1)) // 设置支持的椭圆曲线
//                .setLoggingTag("DTLS-Connector") // 设置日志标签
//                .setConnectionIdGenerator(ConnectionIdGenerator.NONE) // 设置连接 ID 生成器
//                .setConnectionListener((connection) -> {
//                    // 自定义连接监听器
//                })
//                .setSessionListener((session) -> {
//                    // 自定义会话监听器
//                });

            return new DTLSConnector(dtlsConfigBuilder.build());
        }

        // 创建 UDPConnector
        return new UDPConnector(properties.createLocalAddress(), configuration);
    }

    @Nonnull
    @Override
    public NetworkType getType() {
        return DefaultNetworkType.UDP;
    }

    @Nonnull
    @Override
    public Mono<Network> createNetwork(@Nonnull UdpSupportProperties properties) {
        DefaultUdpSupport udpSupport = new DefaultUdpSupport(properties.getId());
        initUpdSupport(udpSupport, properties);
        return Mono.just(udpSupport);
    }

    private void initUpdSupport(DefaultUdpSupport support, UdpSupportProperties properties) {
        try {
            Connector connector = createConnector(properties);
            support.setConnector(connector);
            support.setAddress(properties.createRemoteAddress());
            support.setBindAddress(properties.createLocalAddress());
        } catch (Throwable e) {
            String errorMsg = "UDP初始化失败: " + e.getMessage();
            support.setLastError(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
    }

    @Override
    public Mono<Network> reload(@Nonnull Network network, @Nonnull UdpSupportProperties properties) {
        network.shutdown();
        initUpdSupport(((DefaultUdpSupport) network), properties);
        return Mono.just(network);
    }

    @Nullable
    @Override
    public ConfigMetadata getConfigMetadata() {
        return new DefaultConfigMetadata()
            .add("id", "id", "", new StringType())
            .add("host", "本地地址", "", new StringType())
            .add("port", "本地端口", "", new IntType())
            .add("publicHost", "公网地址", "", new StringType())
            .add("publicPort", "公网端口", "", new IntType())
            .add("privateKeyAlias", "私钥别名", "", new StringType())
            .add("certId", "CA证书", "", new StringType().expand("selector", "cert"))
            .add("secure", "开启TSL", "", new BooleanType())
            .add("parserType", "解析器类型", "", new ObjectType())
            .add("parserConfiguration", "配置解析器", "", new ObjectType());
    }

    @Nonnull
    @Override
    public Mono<UdpSupportProperties> createConfig(@Nonnull NetworkProperties properties) {

        return Mono.defer(() -> {
            UdpSupportProperties clientProperties = FastBeanCopier.copy(properties.getConfigurations(), new UdpSupportProperties());
            clientProperties.setId(properties.getId());
            if (clientProperties.isDtls()) {
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
