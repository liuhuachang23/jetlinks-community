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

import org.hswebframework.web.authorization.Dimension;
import org.hswebframework.web.authorization.DimensionProvider;
import org.hswebframework.web.authorization.DimensionType;
import org.jetlinks.community.openapi.manager.entity.OpenApiClientEntity;
import org.jetlinks.community.openapi.manager.service.LocalOpenApiClientService;
import org.jetlinks.community.openapi.manager.service.OpenApiDimension;
import org.jetlinks.community.openapi.manager.service.OpenApiDimensionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class OpenApiDimensionProvider implements DimensionProvider {

    @Autowired
    private LocalOpenApiClientService apiClientService;

    @Override
    public Flux<? extends DimensionType> getAllType() {
        return Flux.just(OpenApiDimensionType.INSTANCE);
    }

    @Override
    public Flux<? extends Dimension> getDimensionByUserId(String userId) {
        return apiClientService.createQuery()
            .where(OpenApiClientEntity::getUserId, userId)
            .fetch()
            .map(OpenApiDimension::of);
    }

    @Override
    public Mono<? extends Dimension> getDimensionById(DimensionType type, String id) {
        if (!type.isSameType(OpenApiDimensionType.INSTANCE)) {
            return Mono.empty();
        }
        return apiClientService
            .findById(id)
            .map(OpenApiDimension::of);
    }

    @Override
    public Flux<String> getUserIdByDimensionId(String dimensionId) {
        return apiClientService
            .findById(Mono.just(dimensionId))
            .map(OpenApiClientEntity::getUserId)
            .flux();
    }
}
