package org.jetlinks.community.simulator.core;

import org.jetlinks.community.simulator.mqtt.MqttSimulatorProvider;
import org.jetlinks.community.simulator.tcp.TcpSimulatorProvider;

/**
 * 模拟器提供商，用于根据不同的类型来创建对应的模拟器
 *
 * @author zhouhao
 * @see MqttSimulatorProvider
 * @see TcpSimulatorProvider
 * @since 1.6
 */
public interface SimulatorProvider {

    /**
     * @return 模拟器类型
     */
    String getType();

    /**
     * 使用配置创建模拟器
     *
     * @param config 模拟器配置
     * @return 模拟器
     */
    Simulator createSimulator(SimulatorConfig config);

}
