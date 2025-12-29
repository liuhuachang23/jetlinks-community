package org.jetlinks.community.device.service;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.web.crud.service.GenericReactiveCrudService;
import org.jetlinks.community.device.entity.DeviceAssociationEntity;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LocalDeviceAssociationService extends GenericReactiveCrudService<DeviceAssociationEntity, String> {

    private final ReactiveRepository<DeviceAssociationEntity, String> tagRepository;

    public LocalDeviceAssociationService(
        @SuppressWarnings("all")
        ReactiveRepository<DeviceAssociationEntity, String> tagRepository) {
        this.tagRepository = tagRepository;
    }
}
