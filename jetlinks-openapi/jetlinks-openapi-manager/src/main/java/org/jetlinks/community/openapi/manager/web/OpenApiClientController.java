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
package org.jetlinks.community.openapi.manager.web;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.crud.service.ReactiveCrudService;
import org.hswebframework.web.crud.web.reactive.ReactiveServiceCrudController;
import org.jetlinks.community.openapi.manager.entity.OpenApiClientEntity;
import org.jetlinks.community.openapi.manager.service.LocalOpenApiClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("/open-api")
@Resource(id = "open-api", name = "openApi客户端")
@Tag(name = "OpenAPI客户端管理")
public class OpenApiClientController implements ReactiveServiceCrudController<OpenApiClientEntity, String> {

    @Autowired
    public LocalOpenApiClientService openApiClientService;

    @Override
    public ReactiveCrudService<OpenApiClientEntity, String> getService() {
        return openApiClientService;
    }

}
