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
package org.jetlinks.community.logging.access;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.bean.FastBeanCopier;
import org.jetlinks.community.dashboard.MeasurementParameter;
import org.jetlinks.community.dashboard.SimpleMeasurementValue;
import org.jetlinks.community.timeseries.query.Aggregation;
import org.jetlinks.community.timeseries.query.AggregationQueryParam;
import org.jetlinks.core.metadata.types.ArrayType;
import org.jetlinks.core.metadata.types.DateTimeType;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.community.ConfigMetadataConstants;
import org.jetlinks.community.timeseries.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.SmartInitializingSingleton;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static org.jetlinks.core.metadata.SimplePropertyMetadata.of;

@Slf4j
@AllArgsConstructor
public class TimeSeriesAccessLoggerService implements AccessLoggerService, SmartInitializingSingleton {

    public static final TimeSeriesMetric metric = TimeSeriesMetric.of("access_logger");

    private final TimeSeriesManager timeSeriesManager;


    @Override
    public Mono<Void> save(SerializableAccessLog log) {
        Map<String, Object> data = FastBeanCopier.copy(log, new HashMap<>());

        return timeSeriesManager
            .getService(metric)
            .commit(TimeSeriesData.of(log.getRequestTime(), data));
    }

    @Override
    public Mono<PagerResult<SerializableAccessLog>> query(QueryParamEntity queryParam) {
        return timeSeriesManager
            .getService(metric)
            .queryPager(queryParam, ts -> FastBeanCopier.copy(ts.getData(), new SerializableAccessLog()));
    }

    @Override
    public Flux<SerializableAccessLog> queryNoPaging(QueryParamEntity queryParam) {
        return timeSeriesManager
            .getService(metric)
            .query(queryParam)
            .map(ts -> FastBeanCopier.copy(ts.getData(), new SerializableAccessLog()));
    }

    @Override
    public void afterSingletonsInstantiated() {
        timeSeriesManager
            .registerMetadata(
                TimeSeriesMetadata.of(
                    metric,
                    of("requestTime", "请求时间", DateTimeType.GLOBAL),
                    of("responseTime", "响应时间", DateTimeType.GLOBAL),
                    of("target", "请求类", StringType.GLOBAL),
                    of("method", "请求方法", StringType.GLOBAL),
                    of("parameters", "参数", new ObjectType()),
                    of("action", "操作", StringType.GLOBAL),

                    of("ip", "IP地址", StringType.GLOBAL),
                    of("ipRegion", "IP属地", StringType.GLOBAL),
                    of("url", "请求地址", StringType.GLOBAL),
                    of("httpMethod", "HTTP方法", StringType.GLOBAL),
                    of("httpHeaders", "请求头", new ObjectType()),

                    of("exception", "异常信息", new StringType().expand(ConfigMetadataConstants.maxLength, 5120L)),

                    of("bindings", "绑定信息", new ArrayType().elementType(StringType.GLOBAL)),
                    of("creatorId", "创建人", StringType.GLOBAL),
                    of("spanId", "链路跨度ID", StringType.GLOBAL),
                    of("traceId", "链路ID", StringType.GLOBAL),
                    of("context", "上下文", new ObjectType()
                        .addProperty("userId", "用户ID", StringType.GLOBAL)
                        .addProperty("username", "用户名", StringType.GLOBAL)
                    )
                )
            ).subscribe(ignore -> {
                        },
                        error -> log.warn("register access logger metadata error", error));
    }


    /**
     * 按时间范围统计HTTP方法请求量（支持多种时间粒度）
    * @return 统计结果
     */
    @Override
    public Flux<HttpMethodStats> statsByInterval(MeasurementParameter parameter) {
        // 1. 定义所有需要返回的HTTP方法（按需调整）
        List<String> allMethods = List.of("GET", "POST", "PUT", "DELETE", "PATCH");

        // 2. 获取时间格式和构建查询
        String format = parameter.getString("format").orElse("yyyy年MM月dd日");
        DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        AggregationQueryParam queryParam = createHttpMethodQueryParam(format, parameter);

        return timeSeriesManager
            .getService(metric)
            .aggregation(queryParam)
            .collectList()
            .flatMapMany(aggregationDataList -> {
                // 3. 按时间桶分组聚合数据
                Map<String, Map<String, Long>> timeBucketStats = new TreeMap<>();

                // 4. 初始化所有时间桶和方法
                aggregationDataList.forEach(data -> {
                    String timeBucket = data.getString("time", "");
                    String httpMethod = data.getString("httpMethod", "").toUpperCase();
                    long count = data.getLong("count", 0L);

                    // 初始化该时间桶的统计
                    timeBucketStats
                        .computeIfAbsent(timeBucket, k -> new LinkedHashMap<>())
                        .put(httpMethod, count);
                });

                // 5. 补全所有方法和时间桶
                return Flux.fromStream(
                    timeBucketStats.entrySet().stream()
                                   .map(entry -> {
                                       Map<String, Long> stats = new LinkedHashMap<>();

                                       // 先初始化所有方法为0
                                       allMethods.forEach(method -> stats.put(method, 0L));

                                       // 然后填充实际数据
                                       entry.getValue().forEach((method, count) -> {
                                           if (allMethods.contains(method)) {
                                               stats.put(method, count);
                                           }
                                       });

                                       return new HttpMethodStats(entry.getKey(), stats);
                                   })
                                   .sorted(Comparator.comparing(HttpMethodStats::getDate))
                );
            });
    }
    /**
     * 创建HTTP方法统计的聚合查询参数
     *
     * @param format 时间格式
     * @param parameter 测量参数
     * @return 聚合查询参数
     */
    public AggregationQueryParam createHttpMethodQueryParam(String format, MeasurementParameter parameter) {
        return AggregationQueryParam
            .of()
            .agg("httpMethod", "count", Aggregation.COUNT) // 统计HTTP方法出现次数
            .groupBy(parameter.getInterval("time", null), format) // 默认按月分组
            .groupBy("httpMethod", "httpMethod") // 按HTTP方法分组
            .limit(parameter.getInt("limit").orElse(1))
            .from(parameter
                      .getDate("from")
                      .orElse(Date.from(LocalDateTime
                                            .now()
                                            .plusDays(-1)
                                            .atZone(ZoneId.systemDefault())
                                            .toInstant())))
            .to(parameter.getDate("to").orElse(new Date()));
    }

}
