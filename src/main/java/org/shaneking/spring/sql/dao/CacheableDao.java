package org.shaneking.spring.sql.dao;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NonNull;
import org.shaneking.skava.sk.annotation.BeCareful;
import org.shaneking.skava.sk.collect.Tuple;
import org.shaneking.skava.t3.jackson.OM3;
import org.shaneking.spring.sql.annotation.SKCacheEvict;
import org.shaneking.spring.sql.annotation.SKCacheable;
import org.shaneking.spring.sql.entity.CacheableEntity;
import org.shaneking.spring.sql.exception.SqlException;
import org.shaneking.sql.Keyword0;
import org.shaneking.sql.OperationContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

@Repository
public class CacheableDao {
  public static final String FMT_RESULT_NOT_EQUALS_ONE = "Result not equals one : {0} = {1}.";

  @Autowired
  @Getter
  private JdbcTemplate jdbcTemplate;

  public <T extends CacheableEntity> int add(@NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.insertSql();
    return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
  }

  public <T extends CacheableEntity> int cnt(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.selectCountSql();
    return (int) jdbcTemplate.queryForMap(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray()).get("count(1)");
  }

  @BeCareful
  @SKCacheEvict
  public <T extends CacheableEntity> int del(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.deleteSql();
    if (Tuple.getSecond(pair) == null || Tuple.getSecond(pair).size() < 1) {
      return 0;//please use truncate
    } else {
      return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  private <T extends CacheableEntity> int delByIdWithoutCache(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.deleteByIdSql();
    if (Tuple.getSecond(pair) == null || Tuple.getSecond(pair).size() < 1) {
      return 0;
    } else {
      return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int delById(@NonNull Class<T> cacheType, @NonNull String id) {
    try {
      if (Strings.isNullOrEmpty(id)) {
        return 0;
      } else {
        T t = cacheType.newInstance();
        t.setId(id);
        return delByIdWithoutCache(cacheType, t);
      }
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int delByIds(@NonNull Class<T> cacheType, @NonNull List<String> ids) {
    try {
      if (ids.size() == 0) {
        return 0;
      } else {
        T t = cacheType.newInstance();
        Map<String, OperationContent> map = Maps.newHashMap();
        map.put("id", new OperationContent().setOp(Keyword0.IN).setCl(ids));
        t.setWhereOCs(map);
        return delByIdWithoutCache(cacheType, t);
      }
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  private <T extends CacheableEntity> int delByIdAndVersionWithoutCache(@NonNull Class<T> cacheType, @NonNull T t) {
    if (Strings.isNullOrEmpty(t.getId())) {
      return 0;
    } else {
      Tuple.Pair<String, List<Object>> pair = t.deleteByIdAndVersionSql();
      return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int delByIdAndVersion(@NonNull Class<T> cacheType, @NonNull String id, @NonNull Integer version) {
    try {
      T t = cacheType.newInstance();
      t.setVer(version).setId(id);
      return delByIdAndVersionWithoutCache(cacheType, t);
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int modById(@NonNull Class<T> cacheType, @NonNull T t) {
    if (Strings.isNullOrEmpty(t.getId())) {
      return 0;
    } else {
      Tuple.Pair<String, List<Object>> pair = t.updateByIdSql();
      return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int modByIdAndVersion(@NonNull Class<T> cacheType, @NonNull T t) {
    if (Strings.isNullOrEmpty(t.getId())) {
      return 0;
    } else {
      Tuple.Pair<String, List<Object>> pair = t.updateByIdAndVersionSql();
      return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  @SKCacheable
  public <T extends CacheableEntity> List<T> lst(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.selectSql();
    return jdbcTemplate.query(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray(), (resultSet, i) -> {
      try {
        T rst = cacheType.newInstance();
        rst.setSelectList(t.getSelectList());
        rst.mapRow(resultSet);
        return rst;
      } catch (Exception e) {
        throw new SqlException(e);
      }
    });
  }

  private <T extends CacheableEntity> T oneWithoutCache(@NonNull Class<T> cacheType, @NonNull T t, boolean rtnNullIfNotEqualsOne) {
    List<T> lst = this.lst(cacheType, t);
    if (lst.size() == 1) {
      return lst.get(0);
    } else {
      if (rtnNullIfNotEqualsOne) {
        return null;
      } else {
        throw new SqlException(MessageFormat.format(FMT_RESULT_NOT_EQUALS_ONE, cacheType.getName(), OM3.writeValueAsString(t)));
      }
    }
  }

  @SKCacheable
  public <T extends CacheableEntity> T one(@NonNull Class<T> cacheType, @NonNull T t, boolean rtnNullIfNotEqualsOne) {
    return oneWithoutCache(cacheType, t, rtnNullIfNotEqualsOne);
  }

  @SKCacheable
  public <T extends CacheableEntity> T one(@NonNull Class<T> cacheType, @NonNull T t) {
    return oneWithoutCache(cacheType, t, false);
  }

  @SKCacheable
  public <T extends CacheableEntity> T oneById(@NonNull Class<T> cacheType, @NonNull String id, boolean rtnNullIfNotEqualsOne) {
    try {
      T t = cacheType.newInstance();
      t.setId(id);
      return oneWithoutCache(cacheType, t, rtnNullIfNotEqualsOne);
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheable
  public <T extends CacheableEntity> T oneById(@NonNull Class<T> cacheType, @NonNull String id) {
    return oneById(cacheType, id, false);
  }

}
