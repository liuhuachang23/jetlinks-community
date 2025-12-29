package org.jetlinks.community.network.manager.web.request;

import lombok.*;
import org.jetlinks.core.message.codec.http.Header;
import org.jetlinks.core.message.codec.http.HttpRequestMessage;
import org.jetlinks.rule.engine.executor.PayloadType;
import org.springframework.http.HttpMethod;

import java.util.*;

/**
 * @author bsetfeng
 * @since 1.0
 **/
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class HttpMessageRequest {


    //消息体
    @Generated
    private Object payload;
    
    @Generated
    private String url;

    //请求方法
    @Generated
    private HttpMethod method;

    //请求头
    @Generated
    private List<Header> headers;

    //参数
    @Generated
    private Map<String, String> queryParameters;

    //请求类型
    @Generated
    private String contentType;

    private PayloadType requestPayloadType = PayloadType.JSON;

    private PayloadType responsePayloadType = PayloadType.JSON;


    public static HttpMessageRequest of(HttpRequestMessage message, PayloadType type) {
        HttpMessageRequest request = new HttpMessageRequest();
        if (message.getContentType() != null) {
            request.setContentType(message.getContentType().toString());
        }
        request.setHeaders(message.getHeaders());
        request.setMethod(message.getMethod());
        request.setPayload(type.read(message.getPayload()));
        request.setQueryParameters(message.getQueryParameters());
        request.setUrl(message.getUrl());
        return request;
    }


}
