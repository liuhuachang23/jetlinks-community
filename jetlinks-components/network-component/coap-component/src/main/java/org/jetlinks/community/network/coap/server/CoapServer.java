package org.jetlinks.community.network.coap.server;

import org.eclipse.californium.core.server.resources.CoapExchange;
import org.jetlinks.community.network.ServerNetwork;
import reactor.core.publisher.Flux;

/**
 * CoAP 服务接口
 *
 * @author zhouhao
 * @since 1.0
 */
public interface CoapServer extends ServerNetwork {

    /**
     * 订阅CoAP请求
     *
     * @return 请求数据流
     */
    Flux<CoapExchange> subscribe();

    /**
     * 停止服务
     */
    void shutdown();
}
