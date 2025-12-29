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

import org.hswebframework.web.authorization.ReactiveAuthenticationManager;
import org.jetlinks.community.openapi.OpenApiClient;
import org.jetlinks.community.openapi.OpenApiClientManager;
import org.jetlinks.community.openapi.Signature;
import org.jetlinks.community.openapi.manager.entity.OpenApiClientEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Arrays;

@Service
public class LocalOpenApiClientManager implements OpenApiClientManager {

    @Autowired
    private LocalOpenApiClientService openApiClientService;

    @Autowired
    private ReactiveAuthenticationManager authenticationManager;

    @Override
    public Mono<OpenApiClient> getClient(String clientId) {
        return openApiClientService.findById(Mono.just(clientId))
                .filter(OpenApiClientEntity::statusIsEnabled)
                .flatMap(client -> authenticationManager.getByUserId(client.getUserId())
                        .map(auth -> {
                            OpenApiClient openApiClient = new OpenApiClient();
                            openApiClient.setAuthentication(auth);
                            openApiClient.setSignature(Signature.valueOf(client.getSignature().toUpperCase()));
                            openApiClient.setSecureKey(client.getSecureKey());
                            if(StringUtils.hasText(client.getIpWhiteList())){
                                openApiClient.setIpWhiteList(Arrays.asList(client.getIpWhiteList().split("[,;\n]")));
                            }
                            openApiClient.setClientId(client.getId());
                            openApiClient.setClientName(client.getClientName());
                            return openApiClient;
                        }));
    }
}
