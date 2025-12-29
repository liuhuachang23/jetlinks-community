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
package org.jetlinks.community.openapi;

import lombok.Getter;
import lombok.Setter;
import org.hswebframework.web.authorization.Authentication;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class OpenApiClient implements Serializable {

    private String clientId;

    private String clientName;

    private String secureKey;

    private List<String> ipWhiteList;

    private Signature signature;

    private Authentication authentication;

    public boolean verifyIpAddress(String ipAddress) {
        if (CollectionUtils.isEmpty(ipWhiteList) || StringUtils.isEmpty(ipAddress)) {
            return true;
        }
        if (ipAddress.contains(" ")) {
            ipAddress = ipAddress.split("[ ]")[0].trim();
        }
        return ipWhiteList.contains(ipAddress);
    }
}
