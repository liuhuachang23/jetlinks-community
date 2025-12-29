package org.jetlinks.community.auth.web;

import com.alibaba.fastjson.JSON;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.events.AuthorizationSuccessEvent;
import org.hswebframework.web.authorization.token.UserTokenManager;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.community.ValueObject;
import org.jetlinks.community.auth.entity.ThirdPartyUserBindEntity;
import org.jetlinks.community.auth.sso.SsoProperties;
import org.jetlinks.community.auth.sso.ThirdPartyProvider;
import org.jetlinks.community.auth.sso.oauth2.CommonOAuth2SsoProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.http.HttpStatus;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 单点登录相关接口
 */
@RestController
@RequestMapping("/sso")
@Authorize(ignore = true)
@Tag(name = "单点登录")
public class SsoController {

    private final Map<String, ThirdPartyProvider> providers = new HashMap<>();

    private final SsoProperties properties;

    private final ReactiveRedisOperations<String, String> redis;

    private final ReactiveRepository<ThirdPartyUserBindEntity, String> bindRepository;

    private final UserTokenManager userTokenManager;

    public SsoController(SsoProperties properties,
                         ReactiveRedisOperations<String, String> redis,
                         ReactiveRepository<ThirdPartyUserBindEntity, String> bindRepository,
                         UserTokenManager userTokenManager,
                         @Autowired(required = false) List<ThirdPartyProvider> providers) {
        this.properties = properties;
        this.redis = redis;
        this.bindRepository = bindRepository;
        this.userTokenManager = userTokenManager;
        for (CommonOAuth2SsoProvider provider : properties.getOauth2()) {
            this.providers.put(provider.getId(), provider);
        }
        if (!CollectionUtils.isEmpty(providers)) {
            for (ThirdPartyProvider provider : providers) {
                this.providers.put(provider.getId(), provider);
            }
        }
    }

    @Getter
    @Setter
    public static class BindCode {
        private String provider;

        private String providerName;

        private ThirdPartyProvider.NotifyResult result;
    }

    @EventListener
    //处理登录时的bind请求
    public void handleAuthBindEvent(AuthorizationSuccessEvent event) {

        String bindCode = event.getParameter("bindCode").map(String::valueOf).orElse("");
        if (StringUtils.isEmpty(bindCode)) {
            return;
        }
        String redisKey = "sso_bind_code:" + bindCode;
        event.async(
            redis
                .opsForValue()
                .get(redisKey)
                .map(bind -> JSON.parseObject(bind, BindCode.class))
                .flatMap(code -> {
                    //保存绑定关系
                    ThirdPartyUserBindEntity bindEntity = new ThirdPartyUserBindEntity();
                    bindEntity.setBindTime(System.currentTimeMillis());
                    bindEntity.setProvider(code.getProvider());
                    bindEntity.setProviderName(code.getProviderName());
                    bindEntity.setDescription(code.getResult().getDescription());
                    bindEntity.setUserId(event.getAuthentication().getUser().getId());
                    bindEntity.setThirdPartyUserId(code.getResult().getThirdPartyUserId());
                    bindEntity.generateId();
                    return bindRepository.save(bindEntity);
                })
                .then(redis.delete(redisKey))
        );

    }

    @GetMapping("/providers")
    @Operation(summary = "获取支持的SSO服务商标识")
    public Flux<String> getProviders() {
        return Flux.fromIterable(providers.keySet());
    }

    @GetMapping("/{provider}/login")
    @Operation(summary = "跳转第三方登录")
    public Mono<Void> redirectSsoLogin(@PathVariable
                                       @Parameter(description = "SSO服务商标识") String provider,
                                       ServerWebExchange exchange) {
        ThirdPartyProvider partyProvider = providers.get(provider);

        if (partyProvider == null) {
            throw new UnsupportedOperationException("unsupported:" + provider);
        }
        String notifyUrl = properties.getBaseUrl() + "/sso/"+provider+"/notify";

        URI url = partyProvider.getLoginUrl(notifyUrl);

        return Mono.fromRunnable(() -> {
            //重定向到登录地址
            exchange.getResponse().setStatusCode(HttpStatus.FOUND);
            exchange.getResponse().getHeaders().setLocation(url);
        });
    }

    /**
     * 处理来自第三方认证服务的回调请求
     * @param provider SSO服务商标识
     * @param exchange 服务器交换对象，用于获取请求参数和设置响应
     */
    @GetMapping("/{provider}/notify")
    @Operation(summary = "登录结果通知")
    public Mono<Void> handleNotify(@PathVariable
                                   @Parameter(description = "SSO服务商标识") String provider,
                                   ServerWebExchange exchange) {
        // 获取对应的第三方认证服务提供者
        ThirdPartyProvider partyProvider = providers.get(provider);

        // 如果未找到对应的服务提供商，则抛出不支持异常
        if (partyProvider == null) {
            throw new UnsupportedOperationException("unsupported:" + provider);
        }

        // 将查询参数封装为ValueObject对象，并获取来源重定向地址
        ValueObject parameters = ValueObject.of(exchange.getRequest().getQueryParams().toSingleValueMap());
        String sourceRedirect = parameters.getString("redirect", properties.getBaseUrl());

        // 调用第三方服务处理回调逻辑，并根据处理结果执行不同分支
        return partyProvider
                .handleNotify(parameters)
                // 过滤掉没有第三方用户ID的结果
                .filter(result -> StringUtils.hasText(result.getThirdPartyUserId()))
                // 如果没有有效结果则抛出异常
                .switchIfEmpty(Mono.error(() -> new UnsupportedOperationException("unsupported notify")))
                .flatMap(notifyResult ->
                        // 查询该第三方用户是否已经绑定本地用户
                        bindRepository
                                .createQuery()
                                .where(ThirdPartyUserBindEntity::getProvider, provider)
                                .and(ThirdPartyUserBindEntity::getThirdPartyUserId, notifyResult.getThirdPartyUserId())
                                .fetchOne()
                                .map(bind -> {
                                    // 已经绑定了用户：
                                    // 生成一个唯一的临时访问令牌，用于后续的身份验证。
                                    String token = IDGenerator.MD5.generate();
                                    return userTokenManager
                                            //调用 userTokenManager.signIn() 进行用户登录
                                            .signIn(token, "sso-" + provider, bind.getUserId(), notifyResult.getExpiresMillis())
                                            //重定向到token设置页面，携带生成的token和原始重定向地址
                                            //1. Token传递
                                            //将生成的临时访问令牌传递给前端页面，前端可以在后续请求中使用此token进行身份验证
                                            //2. 前端处理
                                            //前端接收到token后，通常会将其存储在localStorage或cookie中
                                            //设置为后续API请求的认证凭证（如Authorization header）
                                            //3. 最终重定向
                                            //redirect参数指明用户最终要访问的页面地址
                                            //完成整个SSO登录流程后，用户会被重定向到最初想要访问的页面
                                            .then(Mono.<Void>fromRunnable(() -> {
                                                String redirect = properties.getTokenSetPageUrl();
                                                // 构造重定向URL并设置响应状态码和Location头
                                                URI uri = URI.create(redirect + "?token=" + token + "&redirect=" + sourceRedirect);
                                                exchange.getResponse().setStatusCode(HttpStatus.FOUND);
                                                exchange.getResponse().getHeaders().setLocation(uri);
                                            }));
                                })
                                .defaultIfEmpty(Mono.defer(() -> {
                                    // 未绑定用户：
                                    // 生成绑定码并跳转到绑定页面
                                    String bindCode = IDGenerator.MD5.generate();
                                    // 创建 BindCode 对象并设置相关信息：
                                    //provider: 第三方服务提供商ID（如 "github", "wechat"）
                                    //providerName: 第三方服务提供商名称（如 "GitHub", "微信"）
                                    //result: 第三方认证结果信息，包含第三方用户ID等
                                    BindCode bindCodeResult = new BindCode();
                                    bindCodeResult.setProvider(provider);
                                    bindCodeResult.setProviderName(partyProvider.getName());
                                    bindCodeResult.setResult(notifyResult);
                                    // 将绑定信息存储到Redis中，有效期5分钟
                                    return redis
                                            .opsForValue()
                                            .set("sso_bind_code:" + bindCode, JSON.toJSONString(bindCodeResult), Duration.ofMinutes(5))
                                            .then(Mono.fromRunnable(() -> {
                                                // 重定向到绑定页面
                                                // 绑定成功后，用户最终会被重定向到原始请求页面
                                                String redirect = properties.getBindPageUrl();
                                                URI uri = URI.create(redirect + "?code=" + bindCode + "&redirect=" + sourceRedirect);
                                                exchange.getResponse().setStatusCode(HttpStatus.FOUND);
                                                exchange.getResponse().getHeaders().setLocation(uri);
                                            }));
                                }))
                                .flatMap(Function.identity()));

    }

}
