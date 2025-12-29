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
package org.jetlinks.community.network.monitor;

import org.jetlinks.community.micrometer.MeterRegistryManager;
import org.springframework.stereotype.Component;

/**
 * @author wangzheng
 * @see
 * @since 1.0
 */
@Component
public class MicrometerNetMonitorSupplier implements NetMonitorSupplier {

    private final MeterRegistryManager meterRegistryManager;

    public MicrometerNetMonitorSupplier(MeterRegistryManager meterRegistryManager) {
        this.meterRegistryManager = meterRegistryManager;
        NetMonitors.register(this);
    }

    @Override
    public NetMonitor getMonitor(String id, String... tags) {
        return new MicrometerNetMonitor(meterRegistryManager.getMeterRegister("net_monitor","target"),
                id,
                tags);
    }
}
