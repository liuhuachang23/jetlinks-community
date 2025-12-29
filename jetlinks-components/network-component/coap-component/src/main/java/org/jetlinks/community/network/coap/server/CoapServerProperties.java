package org.jetlinks.community.network.coap.server;

import lombok.*;
import org.jetlinks.community.ValueObject;
import org.jetlinks.community.network.AbstractServerNetworkConfig;
import org.jetlinks.community.network.resource.NetworkTransport;
import org.jetlinks.community.network.security.Certificate;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * CoAP Server配置信息
 *
 * @author zhouhao
 * @since 1.0
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CoapServerProperties extends AbstractServerNetworkConfig implements ValueObject {

    /**
     * 配置ID
     */
    private String id;

    /**
     * 证书ID
     *
     * @see Certificate#getId()
     */
    private String certId;

    /**
     * 绑定网卡地址
     */
    private String address = "0.0.0.0";

    /**
     * 端口
     */
    private int port;

    /**
     * 是否开启DTLS
     */
    private boolean enableDtls;

    /**
     * 证书私钥别名
     */
    private String privateKeyAlias;

    /**
     * 证书
     */
    private Certificate certificate;

    private Map<String, Object> parserConfiguration = new HashMap<>();


    public void validate() {
    }

    public InetSocketAddress createSocketAddress() {
        if (StringUtils.hasText(address)) {
            return new InetSocketAddress(address, port);
        }

        return new InetSocketAddress(port);

    }

    @Override
    public Map<String, Object> values() {
        return parserConfiguration;
    }

    @Override
    public NetworkTransport getTransport() {
        return NetworkTransport.TCP;
    }

    @Override
    public String getSchema() {
        return "coap";
    }
}
