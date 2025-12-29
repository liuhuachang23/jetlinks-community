package org.jetlinks.community.network.websocket.server;

import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.bean.FastBeanCopier;
import org.jetlinks.community.network.*;
import org.jetlinks.community.network.security.CertificateManager;
import org.jetlinks.community.network.security.VertxKeyCertTrustOptions;
import org.jetlinks.core.metadata.ConfigMetadata;
import org.jetlinks.core.metadata.DefaultConfigMetadata;
import org.jetlinks.core.metadata.types.BooleanType;
import org.jetlinks.core.metadata.types.IntType;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.metadata.types.StringType;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WebSocket服务提供商
 *
 * @author wangzheng
 * @since 1.0
 */
@Component
@Slf4j
public class WebSocketServerProvider implements NetworkProvider<WebSocketServerProperties> {


    private final Vertx vertx;
    private final CertificateManager certificateManager;

    public WebSocketServerProvider(Vertx vertx, CertificateManager certificateManager) {
        this.vertx = vertx;
        this.certificateManager = certificateManager;
    }

//    private Mono<Network> initServer(VertxWebSocketServer server, WebSocketServerProperties properties) {
//        List<HttpServer> instances = new ArrayList<>(properties.getInstance());
//        for (int i = 0; i < properties.getInstance(); i++) {
//            HttpServer httpServer = vertx.createHttpServer(properties.getOptions());
//            instances.add(httpServer);
//        }
//        server.setBindAddress(new InetSocketAddress(properties.getHost(), properties.getPort()));
//        server.setHttpServers(instances);
//        for (HttpServer instance : instances) {
//            instance.listen(properties.getPort(), properties.getHost(), res -> {
//                if (res.succeeded()) {
//                    log.info("startup websocket server [{}] on port: {}", properties.getId(), res
//                        .result()
//                        .actualPort());
//                } else {
//                    server.setLastError(res.cause().getMessage());
//                    log.warn("startup websocket server [{}] error", properties.getId(), res.cause());
//                }
//            });
//        }
//    }

    private Mono<Network> initServer(VertxWebSocketServer server, WebSocketServerProperties properties) {
        return Mono.create(sink -> {
            List<HttpServer> instances = new ArrayList<>(properties.getInstance());
            for (int i = 0; i < properties.getInstance(); i++) {
                HttpServer httpServer = vertx.createHttpServer(properties.getOptions());
                instances.add(httpServer);
            }
            server.setBindAddress(new InetSocketAddress(properties.getHost(), properties.getPort()));
            server.setHttpServers(instances);

            // 计数器跟踪已完成的启动操作
            AtomicInteger completed = new AtomicInteger(0);
            AtomicReference<Throwable> lastError = new AtomicReference<>();
            int totalInstances = instances.size();

            // 如果没有实例需要启动，直接完成
            if (totalInstances == 0) {
                sink.success(server);
                return;
            }

            for (HttpServer instance : instances) {
                instance.listen(properties.getPort(), properties.getHost(), res -> {
                    if (res.succeeded()) {
                        log.info("startup websocket server [{}] on port: {}", properties.getId(), res.result().actualPort());
                    } else {
                        lastError.set(res.cause());
                        server.setLastError(res.cause().getMessage());
                        log.warn("startup websocket server [{}] error", properties.getId(), res.cause());
                    }

                    // 检查是否所有实例都已完成启动
                    if (completed.incrementAndGet() == totalInstances) {
                        // 所有实例都已完成启动操作
                        if (lastError.get() != null) {
                            // 至少有一个实例启动失败
                            sink.error(lastError.get());
                        } else {
                            // 所有实例都启动成功
                            sink.success(server);
                        }
                    }
                });
            }
        });
    }

//    private Mono<Network> initServer(VertxWebSocketServer server, WebSocketServerProperties properties) {
//        return Mono.create(sink -> {
//            List<HttpServer> instances = new ArrayList<>(properties.getInstance());
//            for (int i = 0; i < properties.getInstance(); i++) {
//                HttpServer httpServer = vertx.createHttpServer(properties.getOptions());
//                instances.add(httpServer);
//            }
//            server.setBindAddress(new InetSocketAddress(properties.getHost(), properties.getPort()));
//            server.setHttpServers(instances);
//
//            // 简化处理：只关注第一个实例的启动结果
//            if (!instances.isEmpty()) {
//                HttpServer firstInstance = instances.get(0);
//                firstInstance.listen(properties.getPort(), properties.getHost(), res -> {
//                    if (res.succeeded()) {
//                        log.info("startup websocket server [{}] on port: {}", properties.getId(), res
//                                .result()
//                                .actualPort());
//                        sink.success(server);
//                    } else {
//                        server.setLastError(res.cause().getMessage());
//                        log.warn("startup websocket server [{}] error", properties.getId(), res.cause());
//                        sink.error(res.cause());
//                    }
//                });
//            } else {
//                sink.success(server);
//            }
//        });
//    }

    @Nonnull
    @Override
    public NetworkType getType() {
        return DefaultNetworkType.WEB_SOCKET_SERVER;
    }

    @Nonnull
    @Override
    public Mono<Network> createNetwork(@Nonnull WebSocketServerProperties properties) {
        VertxWebSocketServer server = new VertxWebSocketServer(properties.getId());
        return initServer(server, properties);
    }

    @Override
    public Mono<Network> reload(@Nonnull Network network, @Nonnull WebSocketServerProperties properties) {
        log.debug("reload mqtt server[{}]", properties.getId());
        return initServer((VertxWebSocketServer) network, properties);
    }

    @Nullable
    @Override
    public ConfigMetadata getConfigMetadata() {
        return new DefaultConfigMetadata()
            .add("id", "id", "", new StringType())
            .add("payloadType", "payloadType", "", new ObjectType())
            .add("certId", "证书id", "", new StringType())
            .add("ssl", "是否开启ssl", "", new BooleanType())
            .add("port", "服务端口", "", new IntType())
            .add("host", "服务主机地址", "", new StringType())
            .add("uri", "请求路径", "", new StringType())
            .add("instance", "服务实例数量(线程数)", "", new IntType());
    }

    @Nonnull
    @Override
    public Mono<WebSocketServerProperties> createConfig(@Nonnull NetworkProperties properties) {
        return Mono.defer(() -> {
            WebSocketServerProperties config = FastBeanCopier.copy(properties.getConfigurations(), new WebSocketServerProperties());
            config.setId(properties.getId());

            config.setOptions(new JSONObject(properties.getConfigurations()).toJavaObject(HttpServerOptions.class));

            if (config.isSsl()) {
                config.getOptions().setSsl(true);
                return certificateManager.getCertificate(config.getCertId())
                                         .map(VertxKeyCertTrustOptions::new)
                                         .doOnNext(config.getOptions()::setKeyCertOptions)
                                         .doOnNext(config.getOptions()::setTrustOptions)
                                         .thenReturn(config);
            }
            return Mono.just(config);
        });
    }
}
