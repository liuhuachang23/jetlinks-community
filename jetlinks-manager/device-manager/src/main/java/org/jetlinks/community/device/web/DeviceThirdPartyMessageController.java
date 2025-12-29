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
package org.jetlinks.community.device.web;

import com.google.errorprone.annotations.NoAllocation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Generated;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.QueryAction;
import org.hswebframework.web.authorization.annotation.Resource;
import org.jetlinks.community.device.entity.DeviceProperty;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.jetlinks.community.device.service.data.DeviceDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/device/thirdParty")
@Slf4j
@Authorize(ignore = true)
@Resource(id = "device-instance", name = "设备实例")
@Tag(name = "第三方系统_设备指令API")
public class DeviceThirdPartyMessageController {

    private final LocalDeviceInstanceService instanceService;

    private final DeviceDataService deviceDataService;

    public DeviceThirdPartyMessageController(LocalDeviceInstanceService instanceService, DeviceDataService deviceDataService) {
        this.instanceService = instanceService;
        this.deviceDataService = deviceDataService;
    }

    /**
     * 获取设备全部最新属性
     */
    @GetMapping("/{deviceId:.+}/properties/latest")
    @QueryAction
    @Operation(summary = "获取指定ID设备最新的全部属性")
    public Flux<DeviceProperty> getDeviceLatestProperties(@PathVariable @Parameter(description = "设备ID") String deviceId) {
        return deviceDataService.queryEachOneProperties(deviceId, QueryParamEntity.of());
    }

    /**
     * 获取设备指定的最新属性
     */
    @GetMapping("/{deviceId:.+}/property/{property}")
    @QueryAction
    @Operation(summary = "获取指定ID设备最新的属性")
    public Flux<DeviceProperty> getDeviceLatestProperty(@PathVariable @Parameter(description = "设备ID") String deviceId,
                                                        @PathVariable @Parameter(description = "属性ID") String property) {
        String[] propertyArray = property.split(","); // 手动分割
        return deviceDataService.queryEachOneProperties(deviceId, QueryParamEntity.of(), propertyArray);
    }

    /**
     * 设置设备属性（发送写入指令）
     */
    @PostMapping("/setting/{deviceId}/property")
    @SneakyThrows
    @QueryAction
    public Flux<?> writeProperties(@PathVariable String deviceId, @RequestBody Mono<Map<String, Object>> properties) {
        return properties.flatMapMany(props -> instanceService.writeProperties(deviceId, props));
    }

}
