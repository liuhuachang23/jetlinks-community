package org.jetlinks.community.standalone.configuration.api;

import org.jetlinks.community.openapi.OpenApiClientManager;
import org.jetlinks.community.openapi.interceptor.OpenApiFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class OpenApiConfiguration {

    @Bean
    public OpenApiFilter openApiFilter(OpenApiClientManager clientManager) {
        return new OpenApiFilter(clientManager);
    }

}
