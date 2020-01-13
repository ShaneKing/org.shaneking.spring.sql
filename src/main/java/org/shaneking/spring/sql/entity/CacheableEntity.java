package org.shaneking.spring.sql.entity;

import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.shaneking.skava.lang.String20;
import org.shaneking.sql.OperationContent;
import org.shaneking.sql.entity.SKIdAdtVerEntity;
import org.shaneking.sql.entity.SKIdEntity;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Accessors(chain = true)
@ToString
public class CacheableEntity extends SKIdAdtVerEntity<Map<String, OperationContent>> {
  @Override
  public List<OperationContent> findHavingOCs(@NonNull String fieldName) {
    List<OperationContent> rtnList = Lists.newArrayList();
    OperationContent oc = this.getHavingOCs().get(fieldName);
    if (oc != null) {
      rtnList.add(oc);
    }
    rtnList.addAll(this.getHavingOCs().keySet().stream().filter(s -> s.startsWith(fieldName + String20.UNDERLINE_UNDERLINE)).map(s -> this.getHavingOCs().get(s)).collect(Collectors.toSet()));
    return rtnList;
  }

  @Override
  public List<OperationContent> findWhereOCs(@NonNull String fieldName) {
    List<OperationContent> rtnList = Lists.newArrayList();
    OperationContent oc = this.getWhereOCs().get(fieldName);
    if (oc != null) {
      rtnList.add(oc);
    }
    rtnList.addAll(this.getWhereOCs().keySet().stream().filter(s -> s.startsWith(fieldName + String20.UNDERLINE_UNDERLINE)).map(s -> this.getWhereOCs().get(s)).collect(Collectors.toSet()));
    return rtnList;
  }

  public OperationContent forceOc(@NonNull Map<String, OperationContent> ocMap, @NonNull String field) {
    OperationContent oc = ocMap.get(SKIdEntity.FIELD__ID);
    if (oc == null) {
      oc = new OperationContent();
      ocMap.put(SKIdEntity.FIELD__ID, oc);
    }
    return oc;
  }
}
