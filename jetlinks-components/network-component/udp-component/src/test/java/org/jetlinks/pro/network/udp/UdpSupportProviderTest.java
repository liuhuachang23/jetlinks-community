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
//package org.jetlinks.pro.network.udp;
//
//import io.netty.buffer.Unpooled;
//import lombok.SneakyThrows;
//import org.jetlinks.community.network.Network;
//import org.jetlinks.community.network.udp.UdpMessage;
//import org.jetlinks.community.network.udp.UdpSupport;
//import org.jetlinks.community.network.udp.UdpSupportProperties;
//import org.jetlinks.community.network.udp.UdpSupportProvider;
//import org.jetlinks.community.network.security.Certificate;
//import org.jetlinks.community.network.security.CertificateManager;
//import org.jetlinks.community.network.security.DefaultCertificate;
//import org.junit.jupiter.api.Test;
//import org.springframework.core.io.ClassPathResource;
//import org.springframework.util.StreamUtils;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//import reactor.test.StepVerifier;
//
//import java.nio.charset.StandardCharsets;
//import java.time.Duration;
//import java.util.Arrays;
//import java.util.UUID;
//
//class UdpSupportProviderTest {
//
//
//    @Test
//    void testSimple() {
//
//        UdpSupport server = createManager(UdpSupportProperties.builder()
//            .id("server")
//            .localPort(1211)
//            .build());
//
//        UdpSupport client = createManager(UdpSupportProperties.builder()
//            .id("client")
//            .remoteAddress("localhost")
//            .remotePort(1211)
//            .build());
//
//        UdpMessage message = new UdpMessage(Unpooled.wrappedBuffer("test".getBytes()));
//
//        doTest(server, client, message)
//            .take(1)
//            .map(UdpMessage::getPayload)
//            .map(buf -> buf.toString(StandardCharsets.UTF_8))
//            .doFinally(s -> {
//                server.shutdown();
//                client.shutdown();
//            })
//            .as(StepVerifier::create)
//            .expectNext("test")
//            .verifyComplete();
//
//    }
//
//    @Test
//    void testHugePayload() {
//
//        UdpSupport server = createManager(UdpSupportProperties.builder()
//            .id("server")
//            .localPort(1212)
//            .receiverPacketSize(1024*1024)
//            .build());
//
//        UdpSupport client = createManager(UdpSupportProperties.builder()
//            .id("client")
//            .remoteAddress("localhost")
//            .remotePort(1212)
//            .build());
//
//        StringBuilder payload = new StringBuilder();
//        for (int i = 0; i < 1800; i++) {
//            payload.append(UUID.randomUUID());
//        }
//        System.out.println(payload.length());
//        UdpMessage message = new UdpMessage(Unpooled.wrappedBuffer(payload.toString().getBytes()));
//
//        doTest(server, client, message)
//            .take(1)
//            .map(UdpMessage::getPayload)
//            .map(buf -> buf.toString(StandardCharsets.UTF_8))
//            .doFinally(s -> {
//                server.shutdown();
//                client.shutdown();
//            })
//            .timeout(Duration.ofSeconds(2))
//            .as(StepVerifier::create)
//            .expectNext(payload.toString())
//            .verifyComplete();
//
//    }
//
//    Flux<UdpMessage> doTest(UdpSupport server, UdpSupport client, UdpMessage message) {
//        return Mono.just(message)
//            .doOnNext(msg -> Mono.delay(Duration.ofSeconds(1))
//                .then(client.publish(msg))
//                .subscribe())
//            .thenMany(server.subscribe());
//    }
//
//    @Test
//    @SneakyThrows
//    void testDtls() {
//        DefaultCertificate certificate = new DefaultCertificate("server", "test");
//        DefaultCertificate clientCert = new DefaultCertificate("client", "test");
//
//        byte[] server = StreamUtils.copyToByteArray(new ClassPathResource("server.pem").getInputStream());
//        byte[] client = StreamUtils.copyToByteArray(new ClassPathResource("client.pem").getInputStream());
//
//        byte[] cert = StreamUtils.copyToByteArray(new ClassPathResource("trustStore.pem").getInputStream());
//
//        certificate.initPemKey(Arrays.asList(server), Arrays.asList(server))
//            .initPemTrust(Arrays.asList(cert));
//        clientCert
//            .initPemKey(Arrays.asList(client), Arrays.asList(client))
//            .initPemTrust(Arrays.asList(cert));
//
//        UdpSupport serverUpd = createManager(UdpSupportProperties.builder()
//            .id("server")
//            .localPort(1211)
//            .build(), certificate);
//
//        UdpSupport clientUdp = createManager(UdpSupportProperties.builder()
//            .id("client")
//            .remoteAddress("localhost")
//            .remotePort(1211)
//            .build(), clientCert);
//
//        UdpMessage message = new UdpMessage(Unpooled.wrappedBuffer("test".getBytes()));
//
//        doTest(serverUpd, clientUdp, message)
//            .take(1)
//            .map(UdpMessage::getPayload)
//            .map(buf -> buf.toString(StandardCharsets.UTF_8))
//            .doFinally(s -> {
//                serverUpd.shutdown();
//                clientUdp.shutdown();
//            })
//            .as(StepVerifier::create)
//            .expectNext("test")
//            .verifyComplete();
//    }
//
//
//    UdpSupport createManager(UdpSupportProperties properties, Certificate certificate) {
//        properties.setCertId(certificate.getId());
//        Mono<Network> network = new UdpSupportProvider((id) -> Mono.just(certificate)).createNetwork(properties);
//    }
//
//    UdpSupport createManager(UdpSupportProperties properties) {
//        return new UdpSupportProvider((id) -> Mono.empty()).createNetwork(properties);
//    }
//
//}