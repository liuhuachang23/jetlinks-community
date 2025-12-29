
package org.jetlinks.community.device.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.web.bean.FastBeanCopier;

@Getter
@Setter
public class DevicePropertyDetail extends DeviceProperty {

    @Schema(description = "设备名称")
    private String deviceName;

    public static DevicePropertyDetail of(DeviceProperty property, String deviceName) {
        DevicePropertyDetail details = FastBeanCopier.copy(property, new DevicePropertyDetail());
        details.setDeviceName(deviceName);
        return details;
    }
}