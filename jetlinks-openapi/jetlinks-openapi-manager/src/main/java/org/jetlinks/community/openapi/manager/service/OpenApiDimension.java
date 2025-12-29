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

import lombok.Getter;
import lombok.Setter;
import org.hswebframework.web.authorization.Dimension;
import org.hswebframework.web.authorization.DimensionType;
import org.jetlinks.community.openapi.manager.entity.OpenApiClientEntity;

import java.util.Map;

@Getter
@Setter
public class OpenApiDimension implements Dimension {
    private String id;

    private String name;

    private Map<String,Object> options;
    @Override
    public DimensionType getType() {
        return OpenApiDimensionType.INSTANCE;
    }

    public static OpenApiDimension of(OpenApiClientEntity entity){
        OpenApiDimension dimension=new OpenApiDimension();

        dimension.setId(entity.getId());
        dimension.setName(entity.getClientName());
        return dimension;

    }
}
