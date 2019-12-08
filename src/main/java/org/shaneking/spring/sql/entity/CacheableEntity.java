package org.shaneking.spring.sql.entity;

import lombok.ToString;
import lombok.experimental.Accessors;
import org.shaneking.sql.OperationContent;
import org.shaneking.sql.entity.SKIdAdtVerEntity;

import java.util.Map;

@Accessors(chain = true)
@ToString
public class CacheableEntity extends SKIdAdtVerEntity<Map<String, OperationContent>> {
}
