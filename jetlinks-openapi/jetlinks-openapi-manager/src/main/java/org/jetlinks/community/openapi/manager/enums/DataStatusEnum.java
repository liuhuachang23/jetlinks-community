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
package org.jetlinks.community.openapi.manager.enums;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hswebframework.web.dict.Dict;
import org.hswebframework.web.dict.EnumDict;

/**
 * @author: gaoyf
 * @since: 1.0
 * @Date: 19-11-20 上午10:46
 */
@AllArgsConstructor
@Getter
@Dict("data-status-enum")
@JsonDeserialize(contentUsing = EnumDict.EnumDictJSONDeserializer.class)
public enum DataStatusEnum implements EnumDict<Byte> {
    ENABLED((byte) 1, "正常"),
    DISABLED((byte) 0, "禁用"),
    LOCK((byte) -1, "锁定"),
    DELETED((byte) -10, "删除");

    private Byte value;

    private String text;

}