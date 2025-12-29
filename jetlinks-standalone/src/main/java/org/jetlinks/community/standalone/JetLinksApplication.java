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
package org.jetlinks.community.standalone;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.basic.configuration.EnableAopAuthorize;
import org.hswebframework.web.crud.annotation.EnableEasyormRepository;
import org.hswebframework.web.logging.aop.EnableAccessLogger;
import org.jetlinks.supports.protocol.validator.MethodDeniedClassVisitor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication(scanBasePackages = "org.jetlinks.community", exclude = {
    DataSourceAutoConfiguration.class
})
@EnableCaching
@EnableEasyormRepository("org.jetlinks.community.**.entity")
@EnableAopAuthorize
@EnableAccessLogger
@Slf4j
public class JetLinksApplication {

    public static void main(String[] args) {
        // 放行 System.exit（不建议用于生产环境）
        MethodDeniedClassVisitor.global().removeDenied(System.class, "exit");
        // 放行 ProcessBuilder.start（存在命令注入风险）
        MethodDeniedClassVisitor.global().removeDenied(ProcessBuilder.class, "start");
        // 放行 Runtime.exec（极高风险，慎重放行）
        MethodDeniedClassVisitor.global().removeDenied(Runtime.class, "exec");
        try {
            SpringApplication.run(JetLinksApplication.class, args);
        } catch (Throwable error) {
            System.err.println("startup failed!");
            System.exit(1);
        }
    }



}
