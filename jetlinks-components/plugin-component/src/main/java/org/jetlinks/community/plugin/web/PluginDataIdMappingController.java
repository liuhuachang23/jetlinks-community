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
package org.jetlinks.community.plugin.web;

import com.alibaba.excel.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.ezorm.rdb.mapping.defaults.SaveResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.annotation.DeleteAction;
import org.hswebframework.web.authorization.annotation.QueryAction;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.SaveAction;
import org.jetlinks.community.plugin.impl.id.PluginDataIdMappingEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 插件数据ID映射.
 *
 * @author zhangji 2023/3/2
 */
@Slf4j
@AllArgsConstructor
@RestController
@RequestMapping("/plugin/mapping")
@Resource(id = "plugin-driver", name = "插件驱动管理")
@Tag(name = "插件数据ID映射")
public class PluginDataIdMappingController {

    private final ReactiveRepository<PluginDataIdMappingEntity, String> repository;

    /**
     * 读取原始 JSON 数据
     */
//    private static List<Map<String, Object>> readOriginalJson(MultipartFile file) throws Exception {
//
//        List<Map<String, Object>> result = new ArrayList<>();
//
//        try (Workbook workbook = new XSSFWorkbook(file.getInputStream())) {
//            Sheet sheet = workbook.getSheetAt(0); // 获取第一个工作表
//
//            // 假设第一行是标题，从第二行开始读取数据
//            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
//                Row row = sheet.getRow(i);
//                if (row == null) continue;
//
//                Cell deviceIdCell = row.getCell(0);
//                Cell externalIdCell = row.getCell(1);
//
//                if (deviceIdCell != null && externalIdCell != null) {
//                    Map<String, Object> mapping = new HashMap<>();
//
//                    // 处理deviceId
//                    if (deviceIdCell.getCellType() == CellType.STRING) {
//                        mapping.put("deviceId", deviceIdCell.getStringCellValue());
//                    } else if (deviceIdCell.getCellType() == CellType.NUMERIC) {
//                        mapping.put("deviceId", String.valueOf((int)deviceIdCell.getNumericCellValue()));
//                    }
//
//                    // 处理externalId
//                    if (externalIdCell.getCellType() == CellType.STRING) {
//                        mapping.put("externalId", externalIdCell.getStringCellValue());
//                    } else if (externalIdCell.getCellType() == CellType.NUMERIC) {
//                        mapping.put("externalId", String.valueOf(externalIdCell.getNumericCellValue()));
//                    }
//
//                    result.add(mapping);
//                }
//            }
//        }
//
//        log.info("解析映射文件数据：{}", JSON.toJSON(result));
//        return result;
//    }

    private static List<Map<String, Object>> readOriginalJson(String filePath) throws Exception {
        File file = new File(filePath);
        String jsonStr = FileCopyUtils.copyToString(new FileReader(file));
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonStr, new TypeReference<List<Map<String, Object>>>(){});
    }

    @PostMapping("/{type}/{pluginId:.+}/batchSave")
    @Operation(summary = "批量保存数据ID映射")
    public Mono<Integer> batchSave(@PathVariable @Parameter(description = "插件数据类型") String type,
                                   @PathVariable @Parameter(description = "插件ID") String pluginId,
                                   @RequestParam @Parameter(description = "映射文件全路径地址") String filePath
                                   ) {
        // 1. 读取JSON数据
        return Mono.fromCallable(() -> readOriginalJson(filePath))
                .onErrorResume(e -> {
                    return Mono.error(new RuntimeException("读取数据文件失败"));
                })
                .flatMapMany(Flux::fromIterable)
                // 2. 处理每条记录
                .flatMap(item -> {
                    String deviceId = (String) item.get("deviceId");        //平台设备id
                    String externalId = (String) item.get("externalId");    //需要映射的现场设备id

                    if (StringUtils.isBlank(deviceId) || StringUtils.isBlank(externalId)) {
                        return Mono.just(0); // 返回0表示未插入
                    }

                    // 3. 调用保存接口
                    return save(type, pluginId, deviceId, Mono.just(externalId))
                            .thenReturn(1)  // 成功返回1
                            .onErrorResume(e -> {
                                return Mono.just(0); // 失败返回0
                            });
                }, 5) // 控制并发度
                // 4. 统计成功插入数量
                .reduce(0, Integer::sum);
    }

    @PatchMapping("/{type}/{internalId:.+}")
    @DeleteAction
    @Operation(summary = "解除数据ID映射")
    public Mono<Integer> delete(@PathVariable @Parameter(description = "插件数据类型") String type,
                                 @PathVariable @Parameter(description = "内部数据ID") String internalId) {
        return repository
                .createDelete()
                .where(PluginDataIdMappingEntity::getInternalId, internalId)
                .and(PluginDataIdMappingEntity::getType, type)
                .execute();
    }

    @PatchMapping("/{type}/{pluginId:.+}/{internalId:.+}")
    @SaveAction
    @Operation(summary = "保存数据ID映射")
    public Mono<SaveResult> save(@PathVariable @Parameter(description = "插件数据类型") String type,
                                 @PathVariable @Parameter(description = "插件ID") String pluginId,
                                 @PathVariable @Parameter(description = "内部数据ID") String internalId,
                                 @RequestBody Mono<String> externalId) {
        return externalId
            .map(id -> PluginDataIdMappingEntity.of(pluginId, internalId, type, id))
            .flatMap(entity -> repository
                .createDelete()
                .where(PluginDataIdMappingEntity::getPluginId, pluginId)
                .and(PluginDataIdMappingEntity::getInternalId, internalId)
                .and(PluginDataIdMappingEntity::getType, type)
                .execute()
                .thenReturn(entity))
            .flatMap(repository::save);
    }

    @GetMapping("/{type}/{pluginId:.+}/{internalId:.+}")
    @QueryAction
    @Operation(summary = "获取数据ID映射")
    public Mono<PluginDataIdMappingEntity> queryOne(@PathVariable @Parameter(description = "插件数据类型") String type,
                                                    @PathVariable @Parameter(description = "插件ID") String pluginId,
                                                    @PathVariable @Parameter(description = "内部数据ID") String internalId) {
        return repository
            .createQuery()
            .where(PluginDataIdMappingEntity::getPluginId, pluginId)
            .and(PluginDataIdMappingEntity::getInternalId, internalId)
            .and(PluginDataIdMappingEntity::getType, type)
            .fetchOne();
    }

    @PostMapping("/{type}/_all")
    @QueryAction
    @Operation(summary = "获取指定类型的所有ID映射")
    @Deprecated
    public Flux<PluginDataIdMappingEntity> queryAll(@PathVariable @Parameter(description = "插件数据类型") String type,
                                                    @RequestBody(required = false)
                                                    @Parameter(description = "指定查询的插件数据ID") Mono<List<String>> includes) {
        return includes
            .flatMapMany(externalIds -> repository
                .createQuery()
                .where(PluginDataIdMappingEntity::getType, type)
                .when(!CollectionUtils.isEmpty(externalIds),
                      query -> query.in(PluginDataIdMappingEntity::getExternalId, externalIds))
                .fetch());
    }

    @PostMapping("/{type}/{pluginId:.+}/_all")
    @QueryAction
    @Operation(summary = "获取指定类型的所有ID映射")
    public Flux<PluginDataIdMappingEntity> queryAll(@PathVariable @Parameter(description = "插件数据类型") String type,
                                                    @PathVariable @Parameter(description = "插件ID") String pluginId,
                                                    @RequestBody(required = false)
                                                    @Parameter(description = "指定查询的插件数据ID") Mono<List<String>> includes) {
        return includes
            .flatMapMany(externalIds -> repository
                .createQuery()
                .where(PluginDataIdMappingEntity::getType, type)
                .and(PluginDataIdMappingEntity::getPluginId, pluginId)
                .when(!CollectionUtils.isEmpty(externalIds),
                    query -> query.in(PluginDataIdMappingEntity::getExternalId, externalIds))
                .fetch());
    }

    @PostMapping("/_query")
    @QueryAction
    @Operation(summary = "动态查询ID映射")
    public Flux<PluginDataIdMappingEntity> query(@RequestBody(required = false) Mono<QueryParamEntity> queryParam) {
        return queryParam
                .flatMapMany(query -> repository
                        .createQuery()
                        .setParam(query)
                        .fetch());
    }
}
