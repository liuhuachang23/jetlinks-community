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
package org.jetlinks.community.messaging.rabbitmq;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class SimpleAmqpMessage implements AmqpMessage {

    private String exchange;

    private String routeKey;

    private Map<String, Object> properties;

    private ByteBuf payload;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("exchange:").append(exchange);

        if (payload != null) {
            builder.append(",payload:");
            if (ByteBufUtil.isText(payload, StandardCharsets.UTF_8)) {
                builder.append(payload.toString(StandardCharsets.UTF_8));
            } else {
                builder.append(ByteBufUtil.hexDump(payload));
            }
        }
        return builder.toString();
    }
}
