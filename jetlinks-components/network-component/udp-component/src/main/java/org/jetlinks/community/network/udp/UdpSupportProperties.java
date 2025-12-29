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

import lombok.*;
import org.eclipse.californium.elements.auth.RawPublicKeyIdentity;
import org.jetlinks.community.ValueObject;
import org.jetlinks.community.network.AbstractServerNetworkConfig;
import org.jetlinks.community.network.resource.NetworkTransport;
import org.jetlinks.community.network.security.Certificate;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * UDP支持配置
 *
 * @author zhouhao
 * @since 1.0
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UdpSupportProperties extends AbstractServerNetworkConfig implements ValueObject {

    /**
     * 配置ID
     */
    private String id;

//    /**
//     * 远程地址，不为空时，表示UDP客户端
//     */
//    private String remoteAddress;
//
//    /**
//     * 远程端口
//     */
//    private int remotePort;

//    /**
//     * 本地地址
//     */
//    private String localAddress = "0.0.0.0";
//
//    /**
//     * 本地端口
//     */
//    private int localPort;


    /**
     * 是否开启DTLS
     */
    private boolean dtls;

    /**
     * DTLS证书ID
     *
     * @see Certificate#getId()
     * @see org.jetlinks.pro.network.security.CertificateManager
     */
    private String certId;

    /**
     * 证书私钥别名
     */
    private String privateKeyAlias;

    /**
     * 最大接收包长度,默认65535
     */
    private int receiverPacketSize = 0xffff;

    /**
     * 证书详情
     */
    private transient Certificate certificate;

    private Map<String, Object> parserConfiguration = new HashMap<>();

    public InetSocketAddress createRemoteAddress() {
        if (StringUtils.hasText(publicHost)) {
            return new InetSocketAddress(publicHost, publicPort);
        } else {
            return null;
        }
    }

    public InetSocketAddress createLocalAddress() {
        if (StringUtils.hasText(host)) {
            return new InetSocketAddress(host, port);
        } else {
            return new InetSocketAddress(port);
        }
    }

    @Override
    public NetworkTransport getTransport() {
      return NetworkTransport.UDP;
    }

    @Override
    public String getSchema() {
        return "udp";
    }

    @Override
    public Map<String, Object> values() {
        return parserConfiguration;
    }
}
