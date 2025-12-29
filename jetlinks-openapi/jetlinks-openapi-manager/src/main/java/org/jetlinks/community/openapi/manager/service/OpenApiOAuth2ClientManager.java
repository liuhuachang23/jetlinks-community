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
package org.jetlinks.community.openapi.manager.service;

import lombok.AllArgsConstructor;
import org.hswebframework.web.oauth2.server.OAuth2Client;
import org.hswebframework.web.oauth2.server.OAuth2ClientManager;
import org.jetlinks.community.openapi.manager.entity.OpenApiClientEntity;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@AllArgsConstructor
public class OpenApiOAuth2ClientManager implements OAuth2ClientManager {

    private final LocalOpenApiClientService clientService;


    @Override
    public Mono<OAuth2Client> getClient(String clientId) {

        return clientService
            .findById(clientId)
            .filter(OpenApiClientEntity::clientIsEnableOAuth2)
            .map(OpenApiClientEntity::toOAuth2Client);
    }
}
