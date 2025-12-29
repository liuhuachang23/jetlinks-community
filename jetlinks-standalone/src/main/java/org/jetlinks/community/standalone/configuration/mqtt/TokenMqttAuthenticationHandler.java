package org.jetlinks.community.standalone.configuration.mqtt;

import io.vertx.mqtt.MqttAuth;
import lombok.AllArgsConstructor;
import org.hswebframework.web.authorization.Authentication;
import org.hswebframework.web.authorization.ReactiveAuthenticationManager;
import org.hswebframework.web.authorization.token.UserTokenManager;
import org.hswebframework.web.system.authorization.api.service.reactive.ReactiveUserService;
import org.jetlinks.community.gateway.external.mqtt.MqttAuthenticationHandler;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@AllArgsConstructor
public class TokenMqttAuthenticationHandler implements MqttAuthenticationHandler {

    private final UserTokenManager tokenManager;

    private final ReactiveAuthenticationManager authenticationManager;

    private final ReactiveUserService userService;

//    @Override
//    public Mono<Authentication> handle(String clientId,MqttAuth auth) {
//        return tokenManager
//            .getByToken(clientId)
//            .flatMap(token->authenticationManager.getByUserId(token.getUserId()));
//    }

    @Override
    public Mono<Authentication> handle(String clientId, MqttAuth auth) {
        // 如果账号密码不为空，使用账号密码认证
        if (auth != null && auth.getUsername() != null && !auth.getUsername().isEmpty()){

            return userService.findByUsername(auth.getUsername())
                              .flatMap(user -> {
                                  // 应该使用加密后的密码比较，而不是直接 equals
                                  if (user.getPassword().equals(auth.getPassword())){
                                      return authenticationManager.getByUserId(user.getId());
                                  }
                                  return Mono.empty();
                              });
        }
        // 否则使用 clientId 认证
        else {
            return tokenManager
                .getByToken(clientId)
                .flatMap(token -> authenticationManager.getByUserId(token.getUserId()));
        }
    }

}
