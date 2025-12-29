package org.jetlinks.community.simulator.mqtt;

import io.vertx.core.Vertx;
import lombok.AllArgsConstructor;
import org.jetlinks.community.network.security.CertificateManager;
import org.jetlinks.community.simulator.core.AddressPool;
import org.jetlinks.community.simulator.core.SimulatorConfig;
import org.jetlinks.community.simulator.core.SimulatorListenerBuilder;
import org.jetlinks.community.simulator.core.SimulatorProvider;
import org.springframework.stereotype.Component;

@AllArgsConstructor
@Component
public class MqttSimulatorProvider implements SimulatorProvider {

    private final Vertx vertx;

    private final CertificateManager certificateManager;

    private final SimulatorListenerBuilder listenerBuilder;

    private final AddressPool addressPool;

    @Override
    public String getType() {
        return "mqtt_client";
    }

    @Override
    public MqttSimulator createSimulator(SimulatorConfig config) {

        return new MqttSimulator(vertx, certificateManager, config, listenerBuilder, addressPool);
    }

}
