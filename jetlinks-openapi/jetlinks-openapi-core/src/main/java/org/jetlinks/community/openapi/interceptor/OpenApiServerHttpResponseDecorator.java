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
package org.jetlinks.community.openapi.interceptor;

import org.apache.commons.codec.binary.Hex;
import org.jetlinks.community.openapi.OpenApiClient;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.security.MessageDigest;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings("all")
public class OpenApiServerHttpResponseDecorator extends ServerHttpResponseDecorator {


    public OpenApiServerHttpResponseDecorator(ServerHttpResponse delegate) {
        super(delegate);
    }

    @Override
    public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
        return Mono.deferContextual(ctxView -> {
            OpenApiClient client = ctxView.getOrDefault(OpenApiClient.class, null);
            if (client != null) {
                return Flux.from(body)
                        .collectList()
                        .flatMap(list -> {
                            doSign(client, list);
                            return super.writeWith(Flux.fromIterable(list));
                        });
            } else {
                return super.writeWith(body);
            }
        });
    }

    @Override
    public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
        return Mono.deferContextual(ctxView -> {
            OpenApiClient client = ctxView.getOrDefault(OpenApiClient.class, null);
            if (client != null) {
                return Flux.from(body)
                        .flatMap(Function.identity())
                        .collectList()
                        .flatMap(list -> {
                            doSign(client, list);
                            return super.writeWith(Flux.fromIterable(list));
                        });
            } else {
                return super.writeAndFlushWith(body);
            }
        });
    }


    private void doSign(OpenApiClient client, List<? extends DataBuffer> buffers) {
        MessageDigest digest = client.getSignature().getMessageDigest();
        for (DataBuffer dataBuffer : buffers) {
            digest.update(dataBuffer.asByteBuffer());
        }
        String time = String.valueOf(System.currentTimeMillis());
        digest.update(time.getBytes());
        digest.update(client.getSecureKey().getBytes());

        getHeaders().add("X-Timestamp", time);
        getHeaders().add("X-Sign", Hex.encodeHexString(digest.digest()));
    }

}
