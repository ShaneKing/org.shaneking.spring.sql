package org.shaneking.spring.sql.aspectj;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.shaneking.jackson.databind.OM3;
import org.shaneking.skava.lang.Object0;
import org.shaneking.skava.lang.String0;
import org.shaneking.spring.sql.annotation.EntityCacheable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;

@Aspect
@Component
@Slf4j
public class EntityCacheEvictAspect {
  @Autowired
  private EntityCacheAbstractWrapper entityCacheAbstractWrapper;

  @Pointcut("execution(@org.shaneking.spring.sql.annotation.EntityCacheEvict * *..*.*(..))")
  private void pointcut() {
  }

  @After("pointcut() && @annotation(entityCacheable)")
  public void after(JoinPoint jp, EntityCacheable entityCacheable) throws Throwable {
    if (jp.getArgs().length > entityCacheable.clsIdx() && jp.getArgs()[entityCacheable.clsIdx()] instanceof Class) {
      Class clazz = (Class) jp.getArgs()[entityCacheable.clsIdx()];
      try {
        if (entityCacheable.pKeyIdx() > -1) {
          Object pKeyObj = jp.getArgs()[entityCacheable.pKeyIdx()];
          if (pKeyObj instanceof List) {
            //org.shaneking.spring.sql.dao.CacheableDao.delByIds
            List<String> pKeyList = Strings.isNullOrEmpty(entityCacheable.pKeyPath()) ? (List<String>) pKeyObj : ((List<Object>) pKeyObj).parallelStream().map(o -> String.valueOf(Object0.gs(o, entityCacheable.pKeyPath()))).filter(s -> !String0.isNullOrEmpty(s)).collect(Collectors.toList());
            log.info(MessageFormat.format("{0} - {1}({2}) : {3}", clazz.getName(), EntityCacheUtils.INFO_CODE__CACHE_HIT_PART, entityCacheAbstractWrapper.hdel(clazz.getName(), pKeyList.toArray(new String[0])), OM3.writeValueAsString(pKeyList)));
          } else {
            //org.shaneking.spring.sql.dao.CacheableDao.delById(java.lang.Class<T>, T)
            //org.shaneking.spring.sql.dao.CacheableDao.delById(java.lang.Class<T>, T, java.lang.String)
            String k = String.valueOf(Strings.isNullOrEmpty(entityCacheable.pKeyPath()) ? pKeyObj : Object0.gs(pKeyObj, entityCacheable.pKeyPath()));
            if (String0.isNull2Empty(k)) {
              log.warn(MessageFormat.format("{0} - {1}", jp.getSignature().getName(), EntityCacheUtils.ERR_CODE__ANNOTATION_SETTING_ERROR));
            } else {
              log.info(MessageFormat.format("{0} - {1}({2}) : {3}", clazz.getName(), EntityCacheUtils.INFO_CODE__CACHE_HIT_PART, entityCacheAbstractWrapper.hdel(clazz.getName(), k), k));
            }
          }
        }
      } catch (Throwable e) {
        log.error(String.valueOf(clazz), e);
      }
    } else {
      log.warn(MessageFormat.format("{0} - {1}", jp.getSignature().getName(), EntityCacheUtils.ERR_CODE__ANNOTATION_SETTING_ERROR));
    }
  }
}
