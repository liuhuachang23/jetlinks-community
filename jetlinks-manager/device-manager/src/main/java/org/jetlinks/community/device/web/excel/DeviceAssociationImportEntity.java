package org.jetlinks.community.device.web.excel;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.hswebframework.reactor.excel.ExcelHeader;
import org.hswebframework.reactor.excel.CellDataType;

import java.util.ArrayList;
import java.util.List;

@Data
@Schema(description = "设备关联导入实体")
public class DeviceAssociationImportEntity {

    @org.jetlinks.community.io.excel.annotation.ExcelHeader(value = "关联设备ID")
    @Schema(description = "关联设备ID")
    private String associationId;

    @org.jetlinks.community.io.excel.annotation.ExcelHeader(value = "关联方名称")
    @Schema(description = "关联方名称")
    private String associationName;

    @org.jetlinks.community.io.excel.annotation.ExcelHeader(value = "被关联设备ID")
    @Schema(description = "被关联设备ID")
    private String beAssociationId;

    @org.jetlinks.community.io.excel.annotation.ExcelHeader(value = "被关联方名称")
    @Schema(description = "被关联方名称")
    private String beAssociationName;

    public static List<ExcelHeader> getTemplateHeaders() {
        List<ExcelHeader> headers = new ArrayList<>();
        headers.add(new ExcelHeader("associationId", "关联设备ID", CellDataType.STRING));
        headers.add(new ExcelHeader("associationName", "关联方名称", CellDataType.STRING));
        headers.add(new ExcelHeader("beAssociationId", "被关联设备ID", CellDataType.STRING));
        headers.add(new ExcelHeader("beAssociationName", "被关联方名称", CellDataType.STRING));
        return headers;
    }

    public static DeviceAssociationImportEntity getTemplateExample() {
        DeviceAssociationImportEntity entity = new DeviceAssociationImportEntity();
        entity.setAssociationId("示例设备ID1");
        entity.setAssociationName("示例设备名称1");
        entity.setBeAssociationId("示例设备ID2");
        entity.setBeAssociationName("示例设备名称2");
        return entity;
    }
}
