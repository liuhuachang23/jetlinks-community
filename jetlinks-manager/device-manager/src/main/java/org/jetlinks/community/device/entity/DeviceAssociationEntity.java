package org.jetlinks.community.device.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.ezorm.rdb.mapping.annotation.Comment;
import org.hswebframework.web.api.crud.entity.GenericEntity;
import org.hswebframework.web.api.crud.entity.RecordCreationEntity;
import org.hswebframework.web.api.crud.entity.RecordModifierEntity;
import org.hswebframework.web.crud.annotation.EnableEntityEvent;
import org.hswebframework.web.crud.generator.Generators;
import org.hswebframework.web.validator.CreateGroup;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Index;
import javax.persistence.Table;

@Getter
@Setter
@Table(name = "dev_device_association", indexes = {
    @Index(name = "idx_association_id", columnList = "association_id"),
    @Index(name = "idx_be_association_id", columnList = "be_association_id")
})
@Comment("设备关联设备表")
@EnableEntityEvent
public class DeviceAssociationEntity extends GenericEntity<String> implements RecordCreationEntity, RecordModifierEntity {

    @Override
    @GeneratedValue(generator = Generators.SNOW_FLAKE)
    @Pattern(regexp = "^[0-9a-zA-Z_\\-]+$", message = "ID只能由数字,字母,下划线和中划线组成", groups = CreateGroup.class)
    @Schema(description = "设备ID(只能由数字,字母,下划线和中划线组成)")
    public String getId() {
        return super.getId();
    }

    @Column(name = "association_id", length = 64, updatable = false)
    @NotBlank(message = "关联设备ID不能为空", groups = CreateGroup.class)
    private String associationId;

    @Column(name = "association_name")
    @Schema(description = "关联方名称")
    private String associationName;

    @Column(name = "be_association_id", length = 64, updatable = false)
    @NotBlank(message = "被关联设备ID不能为空", groups = CreateGroup.class)
    private String beAssociationId;

    @Column(name = "be_association_name")
    @Schema(description = "被关联方名称")
    private String beAssociationName;

    @Override
    public String getCreatorId() {
        return "";
    }

    @Override
    public void setCreatorId(String creatorId) {

    }

    @Override
    public Long getCreateTime() {
        return 0L;
    }

    @Override
    public void setCreateTime(Long createTime) {

    }

    @Override
    public String getModifierId() {
        return "";
    }

    @Override
    public void setModifierId(String modifierId) {

    }

    @Override
    public Long getModifyTime() {
        return 0L;
    }

    @Override
    public void setModifyTime(Long modifyTime) {

    }
}