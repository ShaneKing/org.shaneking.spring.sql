package org.shaneking.spring.sql.entity;

import com.google.common.collect.Maps;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.shaneking.skava.lang.String20;
import org.shaneking.sql.OperationContent;
import org.shaneking.sql.entity.SKIdAdtVerEntity;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Accessors(chain = true)
@ToString
public class CacheableEntity extends SKIdAdtVerEntity<Map<String, OperationContent>> {
  @Override
  public List<OperationContent> findHavingOCs(@NonNull String fieldName) {
    return this.getHavingOCs().keySet().parallelStream().filter(s -> s.equals(fieldName) || s.startsWith(fieldName + String20.UNDERLINE_UNDERLINE)).map(s -> this.getHavingOCs().get(s)).collect(Collectors.toList());
  }

  @Override
  public List<OperationContent> findWhereOCs(@NonNull String fieldName) {
    return this.getWhereOCs().keySet().parallelStream().filter(s -> s.equals(fieldName) || s.startsWith(fieldName + String20.UNDERLINE_UNDERLINE)).map(s -> this.getWhereOCs().get(s)).collect(Collectors.toList());
  }

  public OperationContent forceHavingOc(@NonNull String field) {
    Map<String, OperationContent> ocMap = this.getHavingOCs();
    if (ocMap == null) {
      ocMap = Maps.newHashMap();
      this.setHavingOCs(ocMap);
    }
    return forceOc(ocMap, field);
  }

  public OperationContent forceOc(@NonNull Map<String, OperationContent> ocMap, @NonNull String field) {
    OperationContent oc = ocMap.get(field);
    if (oc == null) {
      oc = new OperationContent();
      ocMap.put(field, oc);
    }
    return oc;
  }

  public OperationContent forceWhereOc(@NonNull String field) {
    Map<String, OperationContent> ocMap = this.getWhereOCs();
    if (ocMap == null) {
      ocMap = Maps.newHashMap();
      this.setWhereOCs(ocMap);
    }
    return forceOc(ocMap, field);
  }
}
