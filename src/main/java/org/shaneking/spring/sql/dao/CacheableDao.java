package org.shaneking.spring.sql.dao;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class CacheableDao {
  public static final String FMT_RESULT_NOT_EQUALS_ONE = "Result not equals one : {0} = {1}.";

  @Autowired
  @Getter
  private JdbcTemplate jdbcTemplate;

  public <T extends CacheableEntity> int add(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.insertSql();
    log.info(OM3.writeValueAsString(pair));
    return this.getJdbcTemplate().update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
  }

  public <T extends CacheableEntity> int cnt(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.selectCountSql();
    log.info(OM3.writeValueAsString(pair));
    return (int) this.getJdbcTemplate().queryForMap(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray()).get(Keyword0.COUNT_1_);
  }

  public <T extends CacheableEntity> int ids(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.selectIdsSql();
    log.info(OM3.writeValueAsString(pair));
    return (int) this.getJdbcTemplate().queryForMap(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray()).get(Keyword0.GROUP__CONCAT_ID_);
  }

  @SKCacheEvict(pKeyIdx = 1, pKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> int delById(@NonNull Class<T> cacheType, @NonNull T t) {
    try {
      if (Strings.isNullOrEmpty(t.getId())) {
        throw new SqlException();
      } else {
        Tuple.Pair<String, List<Object>> pair = t.deleteSql();
        log.info(OM3.writeValueAsString(pair));
        return this.getJdbcTemplate().update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
      }
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheEvict(pKeyIdx = 2)
  public <T extends CacheableEntity> int delById(@NonNull Class<T> cacheType, T t, @NonNull String id) {
    try {
      if (Strings.isNullOrEmpty(id)) {
        throw new SqlException();
      } else {
        if (t == null) {
          t = cacheType.newInstance();
        }
        t.forceWhereOc(SKIdEntity.FIELD__ID).resetId(id);
        Tuple.Pair<String, List<Object>> pair = t.deleteSql();
        log.info(OM3.writeValueAsString(pair));
        return this.getJdbcTemplate().update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
      }
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheEvict(pKeyIdx = 2)
  public <T extends CacheableEntity> int delByIds(@NonNull Class<T> cacheType, T t, @NonNull List<String> ids) {
    try {
      if (ids.size() == 0) {
        return 0;
      } else {
        if (t == null) {
          t = cacheType.newInstance();
        }
        t.forceWhereOc(SKIdEntity.FIELD__ID).resetIds(ids);
        Tuple.Pair<String, List<Object>> pair = t.deleteSql();
        log.info(OM3.writeValueAsString(pair));
        return this.getJdbcTemplate().update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
      }
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  @SKCacheEvict(pKeyIdx = 2)
  public <T extends CacheableEntity> int modByIdsVer(@NonNull Class<T> cacheType, @NonNull T t, @NonNull List<String> ids) {
    if (ids.size() == 0) {
      throw new SqlException();
    } else {
      t.forceWhereOc(SKIdEntity.FIELD__ID).resetIds(ids);
      Tuple.Pair<String, List<Object>> pair = t.updateSql();
      log.info(OM3.writeValueAsString(pair));
      return this.getJdbcTemplate().update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  @SKCacheEvict(pKeyIdx = 1, pKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> int modByIdVer(@NonNull Class<T> cacheType, @NonNull T t) {
    if (Strings.isNullOrEmpty(t.getId())) {
      throw new SqlException();
    } else {
      Tuple.Pair<String, List<Object>> pair = t.updateSql();
      log.info(OM3.writeValueAsString(pair));
      return this.getJdbcTemplate().update(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray());
    }
  }

  @SKCacheable(rKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> List<T> lst(@NonNull Class<T> cacheType, @NonNull T t) {
    return lstWithoutCache(cacheType, t);
  }

  //can't with t. if add t parameter, cache will over
  @SKCacheable(pKeyIdx = 1, rKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> List<T> lstByIds(@NonNull Class<T> cacheType, @NonNull List<String> ids) {
    try {
      T t = cacheType.newInstance();
      t.forceWhereOc(SKIdEntity.FIELD__ID).resetIds(ids);
      return lstWithoutCache(cacheType, t);
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }

  private <T extends CacheableEntity> List<T> lstWithoutCache(@NonNull Class<T> cacheType, @NonNull T t) {
    Tuple.Pair<String, List<Object>> pair = t.selectSql();
    log.info(OM3.writeValueAsString(pair));
    return this.getJdbcTemplate().query(Tuple.getFirst(pair), Tuple.getSecond(pair).toArray(), (resultSet, i) -> {
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

  @SKCacheable(rKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> T one(@NonNull Class<T> cacheType, @NonNull T t) {
    return oneWithoutCache(cacheType, t, false);
  }

  @SKCacheable(rKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> T one(@NonNull Class<T> cacheType, @NonNull T t, boolean rtnNullIfNotEqualsOne) {
    return oneWithoutCache(cacheType, t, rtnNullIfNotEqualsOne);
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

  @SKCacheable(pKeyIdx = 1, rKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> T oneById(@NonNull Class<T> cacheType, @NonNull String id) {
    return oneByIdWithoutCache(cacheType, id, false);
  }

  @SKCacheable(pKeyIdx = 1, rKeyPath = SKIdEntity.FIELD__ID)
  public <T extends CacheableEntity> T oneById(@NonNull Class<T> cacheType, @NonNull String id, boolean rtnNullIfNotEqualsOne) {
    return oneByIdWithoutCache(cacheType, id, rtnNullIfNotEqualsOne);
  }

  private <T extends CacheableEntity> T oneByIdWithoutCache(@NonNull Class<T> cacheType, @NonNull String id, boolean rtnNullIfNotEqualsOne) {
    try {
      T t = cacheType.newInstance();
      t.setId(id);
      return oneWithoutCache(cacheType, t, rtnNullIfNotEqualsOne);
    } catch (Exception e) {
      throw new SqlException(e);
    }
  }
}
