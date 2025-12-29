package org.jetlinks.community.logging.access;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

// 在同一个包下创建 HttpMethodStats.java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HttpMethodStats {
    /**
     * 统计日期（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:00:00）
     */
    private String date;

    /**
     * HTTP方法统计结果
     * key: HTTP方法（GET, POST, PUT, DELETE, PATCH等）
     * value: 请求数量
     */
    private Map<String, Long> stats;

    public Long getTotal() {
        return stats.values().stream().mapToLong(Long::longValue).sum();
    }

    public Long getMethodCount(String method) {
        return stats.getOrDefault(method.toUpperCase(), 0L);
    }
}