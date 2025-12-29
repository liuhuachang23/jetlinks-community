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

package org.jetlinks.community.network.udp.executor;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.community.network.PubSubType;
import org.jetlinks.rule.engine.executor.PayloadType;
import org.springframework.util.Assert;

@Getter
@Setter
public class UdpNodeConfiguration {

    private String clientId;

    private PubSubType type;

    private PayloadType sendPayloadType;

    private PayloadType subPayloadType;

    public void validate() {
        Assert.hasText(clientId, "clientId can not be empty!");
        Assert.notNull(type, "type can not be null!");

    }
}
