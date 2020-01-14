package org.shaneking.spring.sql.aspectj;

import lombok.NonNull;

import java.util.List;
import java.util.Map;

public abstract class SKCacheAbstractWrapper {
  @NonNull
  public abstract List<String> hmget(@NonNull String key, String... fields);

  @NonNull
  public abstract String hget(@NonNull String key, @NonNull String field);

  public abstract String hset(@NonNull String key, @NonNull String field, @NonNull String value);

  public abstract String hmset(@NonNull String key, @NonNull Map<String, String> map);

}
