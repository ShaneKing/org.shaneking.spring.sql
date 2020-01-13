package org.shaneking.spring.sql.dao;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import org.shaneking.jackson.databind.OM3;
import org.shaneking.skava.persistence.Tuple;
import org.shaneking.spring.sql.annotation.SKCacheEvict;
import org.shaneking.spring.sql.annotation.SKCacheable;
import org.shaneking.spring.sql.entity.CacheableEntity;
import org.shaneking.spring.sql.exception.SqlException;
import org.shaneking.sql.Keyword0;
import org.shaneking.sql.entity.SKIdEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.text.MessageFormat;
import java.util.List;

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
    return (int) jdbcTemplate.queryForMap(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray()).get(Keyword0.COUNT_1_);
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int delById(@NonNull Class<T> cacheType, T t, @NonNull String id) {
    try {
      if (Strings.isNullOrEmpty(id)) {
        throw new SqlException();
      } else {
        if (t == null) {
          t = cacheType.newInstance();
        }
        t.forceOc(t.getWhereOCs(), SKIdEntity.FIELD__ID).forceSomeId(id);
        Tuple.Pair<String, List<Object>> pair = t.deleteSql();
        return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
      }
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int delByIds(@NonNull Class<T> cacheType, T t, @NonNull List<String> ids) {
    try {
      if (ids.size() == 0) {
        return 0;
      } else {
        if (t == null) {
          t = cacheType.newInstance();
        }
        t.forceOc(t.getWhereOCs(), SKIdEntity.FIELD__ID).forceSomeIds(ids);
        Tuple.Pair<String, List<Object>> pair = t.deleteSql();
        return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
      }
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int modById(@NonNull Class<T> cacheType, @NonNull T t, @NonNull String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new SqlException();
    } else {
      t.forceOc(t.getWhereOCs(), SKIdEntity.FIELD__ID).forceSomeId(id);
      Tuple.Pair<String, List<Object>> pair = t.updateSql();
      return jdbcTemplate.update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  @SKCacheEvict
  public <T extends CacheableEntity> int modByIds(@NonNull Class<T> cacheType, @NonNull T t, @NonNull List<String> ids) {
    if (ids.size() == 0) {
      throw new SqlException();
    } else {
      t.forceOc(t.getWhereOCs(), SKIdEntity.FIELD__ID).forceSomeIds(ids);
      Tuple.Pair<String, List<Object>> pair = t.updateSql();
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

  @SKCacheable
  public <T extends CacheableEntity> T one(@NonNull Class<T> cacheType, @NonNull T t, boolean rtnNullIfNotEqualsOne) {
    return oneWithoutCache(cacheType, t, rtnNullIfNotEqualsOne);
  }

  @SKCacheable
  public <T extends CacheableEntity> T one(@NonNull Class<T> cacheType, @NonNull T t) {
    return oneWithoutCache(cacheType, t, false);
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
  public <T extends CacheableEntity> T oneById(@NonNull Class<T> cacheType, T t, @NonNull String id, boolean rtnNullIfNotEqualsOne) {
    return oneByIdWithoutCache(cacheType, t, id, rtnNullIfNotEqualsOne);
  }

  @SKCacheable
  public <T extends CacheableEntity> T oneById(@NonNull Class<T> cacheType, T t, @NonNull String id) {
    return oneByIdWithoutCache(cacheType, t, id, false);
  }

  private <T extends CacheableEntity> T oneByIdWithoutCache(@NonNull Class<T> cacheType, T t, @NonNull String id, boolean rtnNullIfNotEqualsOne) {
    try {
      if (t == null) {
        t = cacheType.newInstance();
      }
      t.setId(id);
      return oneWithoutCache(cacheType, t, rtnNullIfNotEqualsOne);
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

}
