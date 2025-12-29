package org.jetlinks.community.device.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.jetlinks.community.io.excel.ImportExportService;
import org.jetlinks.community.io.file.FileManager;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.SaveAction;
import org.hswebframework.web.crud.web.reactive.ReactiveServiceCrudController;
import org.hswebframework.web.exception.BusinessException;
import org.jetlinks.community.device.entity.DeviceAssociationEntity;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.device.entity.DeviceProperty;
import org.jetlinks.community.device.entity.DevicePropertyDetail;
import org.jetlinks.community.device.web.excel.DeviceAssociationImportEntity;
import org.jetlinks.community.device.service.LocalDeviceAssociationService;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.jetlinks.community.device.service.data.DeviceDataService;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping({"/device-association", "/device/association"})
@Authorize
@Resource(id = "device-association", name = "设备关联")
@Slf4j
@Tag(name = "设备关联接口")
public class DeviceAssociationController implements
    ReactiveServiceCrudController<DeviceAssociationEntity, String> {

    private final FileManager fileManager;

    private final ImportExportService importExportService;

    @Getter
    private final LocalDeviceAssociationService service;

    private final LocalDeviceInstanceService deviceInstanceService;

    private final DeviceDataService deviceDataService;

    public DeviceAssociationController(FileManager fileManager, ImportExportService importExportService, LocalDeviceAssociationService service, LocalDeviceInstanceService deviceInstanceService, DeviceDataService deviceDataService) {
        this.fileManager = fileManager;
        this.importExportService = importExportService;
        this.service = service;
        this.deviceInstanceService = deviceInstanceService;
        this.deviceDataService = deviceDataService;
    }

    @GetMapping("/{associationId}/property/{property}")
    @Operation(summary = "获取指定ID设备关联的设备最新的属性")
    public Mono<DevicePropertyDetail> getUrl(@PathVariable String associationId, @PathVariable String property) {
        return service.createQuery()
                      .where(DeviceAssociationEntity::getAssociationId, associationId)
                      .fetchOne()
                      .switchIfEmpty(Mono.error(new BusinessException("未找到关联设备信息")))
                      .flatMap(entity -> {
                          // 获取设备属性信息
                          Mono<DeviceProperty> propertyMono = deviceDataService.queryEachOneProperties(
                                                                                   entity.getBeAssociationId(),
                                                                                   QueryParamEntity.of(),
                                                                                   property
                                                                               )
                                                                               .take(1)
                                                                               .singleOrEmpty()
                                                                               .switchIfEmpty(Mono.error(new BusinessException("未找到设备属性信息")));

                          // 获取设备实例信息以获取设备名称
                          Mono<String> deviceNameMono = deviceInstanceService.findById(entity.getBeAssociationId())
                                                                             .map(DeviceInstanceEntity::getName)
                                                                             .defaultIfEmpty("未知设备");

                          // 组装返回结果
                          return Mono.zip(propertyMono, deviceNameMono)
                                     .map(tuple -> DevicePropertyDetail.of(tuple.getT1(), tuple.getT2()));
                      })
                      .onErrorResume(error -> {
                          log.error("获取设备URL属性失败, associationId: {}", associationId, error);
                          return Mono.error(new BusinessException("获取设备URL属性失败: " + error.getMessage()));
                      });
    }

    @PostMapping("/importCustom")
    @SaveAction
    @Operation(summary = "导入自定义格式文件")
    public Mono<String> importCustom(
        @RequestParam(required = false) String fileUrl,
        @RequestParam(required = false) String fileId) {

        return importExportService.getInputStream(fileUrl)
                                  .flatMap(inputStream -> {
                                      try (Workbook workbook = WorkbookFactory.create(inputStream)) { // 自动识别 .xls 或 .xlsx
                                          Sheet sheet = workbook.getSheetAt(0); // 第一个工作表
                                          List<DeviceAssociationImportEntity> dataList = new ArrayList<>();

                                          for (int i = 1; i <= sheet.getLastRowNum(); i++) { // 跳过表头
                                              Row row = sheet.getRow(i);
                                              if (row != null) {
                                                  DeviceAssociationImportEntity entity = new DeviceAssociationImportEntity();
                                                  entity.setAssociationId(getCellValue(row.getCell(0)));
                                                  entity.setAssociationName(getCellValue(row.getCell(1)));
                                                  entity.setBeAssociationId(getCellValue(row.getCell(2)));
                                                  entity.setBeAssociationName(getCellValue(row.getCell(3)));
                                                  dataList.add(entity);
                                              }
                                          }
                                          return Mono.just("导入成功，共处理 " + dataList.size() + " 条数据");
                                      } catch (Exception e) {
                                          return Mono.error(new RuntimeException("Excel 文件解析失败: " + e.getMessage()));
                                      }
                                  });
    }

    // 辅助方法：获取单元格值（兼容数字、字符串、空值）
    private String getCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                return String.valueOf((int) cell.getNumericCellValue()); // 如果是整数
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            default:
                return "";
        }
    }
}

