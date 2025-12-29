package org.jetlinks.community.rule.engine.alarm;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AlarmLevelCount {
    private int level;
    private long value;
}
