package org.shaneking.spring.sql.aspectj;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.shaneking.jackson.databind.OM3;
import org.shaneking.skava.lang.Object0;
import org.shaneking.skava.lang.String0;
import org.shaneking.spring.sql.annotation.EntityCacheable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Aspect
@Component
@Slf4j
public class EntityCacheableAspect {
  @Autowired
  private EntityCacheAbstractWrapper entityCacheAbstractWrapper;

  @Pointcut("execution(@org.shaneking.spring.sql.annotation.EntityCacheable * *..*.*(..))")
  private void pointcut() {
  }

  @Around("pointcut() && @annotation(entityCacheable)")
  public Object around(ProceedingJoinPoint pjp, EntityCacheable entityCacheable) throws Throwable {
    Object rtn = null;
    List<Object> rtnList = null;
    List<Object> argList = null;
    if (pjp.getArgs().length > entityCacheable.clsIdx() && pjp.getArgs()[entityCacheable.clsIdx()] instanceof Class) {
      Class clazz = (Class) pjp.getArgs()[entityCacheable.clsIdx()];
      try {
        if (entityCacheable.pKeyIdx() > -1) {
          Object pKeyObj = pjp.getArgs()[entityCacheable.pKeyIdx()];
          if (pKeyObj instanceof List) {
            //org.shaneking.spring.sql.dao.CacheableDao.lstByIds
            List<String> pKeyList = Strings.isNullOrEmpty(entityCacheable.pKeyPath()) ? (List<String>) pKeyObj : ((List<Object>) pKeyObj).parallelStream().map(o -> String.valueOf(Object0.gs(o, entityCacheable.pKeyPath()))).filter(s -> !String0.isNullOrEmpty(s)).collect(Collectors.toList());
            List<String> cachedList = entityCacheAbstractWrapper.hmget(clazz.getName(), pKeyList.toArray(new String[0]));
            if (cachedList.size() > 0) {
              //compile error
//              rtnList = cachedList.parallelStream().filter(s -> !Strings.isNullOrEmpty(s)).map(s -> OM3.readValue(s, clazz, true)).filter(o -> o != null).collect(Collectors.toList());
              rtnList = cachedList.parallelStream().filter(s -> !Strings.isNullOrEmpty(s)).collect(ArrayList::new, (a, s) -> {
                Object o = OM3.readValue(s, clazz, true);
                if (o != null) {
                  a.add(o);
                }
              }, ArrayList::addAll);
              Set<String> cachedKeySet = rtnList.parallelStream().map(o -> String.valueOf(Object0.gs(o, entityCacheable.rKeyPath()))).filter(s -> !String0.isNullOrEmpty(s)).collect(Collectors.toSet());
              argList = ((List<Object>) pKeyObj).parallelStream().filter(o -> {
                String k = String.valueOf(Strings.isNullOrEmpty(entityCacheable.pKeyPath()) ? o : Object0.gs(o, entityCacheable.pKeyPath()));
                return !cachedKeySet.contains(k);
              }).collect(Collectors.toList());
            }
          } else {
            //org.shaneking.spring.sql.dao.CacheableDao.oneById(java.lang.Class<T>, java.lang.String)
            //org.shaneking.spring.sql.dao.CacheableDao.oneById(java.lang.Class<T>, java.lang.String, boolean)
            String k = String.valueOf(Strings.isNullOrEmpty(entityCacheable.pKeyPath()) ? pKeyObj : Object0.gs(pKeyObj, entityCacheable.pKeyPath()));
            if (String0.isNull2Empty(k)) {
              log.warn(MessageFormat.format("{0} - {1}", pjp.getSignature().getName(), EntityCacheUtils.ERR_CODE__ANNOTATION_SETTING_ERROR));
            } else {
              String cached = entityCacheAbstractWrapper.hget(clazz.getName(), k);
              if (!Strings.isNullOrEmpty(cached)) {
                log.info(MessageFormat.format("{0} - {1} : {2}", clazz.getName(), EntityCacheUtils.INFO_CODE__CACHE_HIT_ALL, cached));
                rtn = OM3.readValue(cached, clazz, true);
              }
            }
          }
        }
      } catch (Throwable e) {
        log.error(String.valueOf(clazz), e);
      }
      if (rtn == null) {
        if (argList != null && argList.size() == 0) {
          log.info(MessageFormat.format("{0} - {1}({2}) : {3}", clazz.getName(), EntityCacheUtils.INFO_CODE__CACHE_HIT_ALL, rtnList.size(), OM3.writeValueAsString(rtnList)));
          rtn = rtnList;
        } else {
          if (argList != null && argList.size() > 0 && entityCacheable.pKeyIdx() > -1) {
            log.info(MessageFormat.format("{0} - {1}({2}) : {3}", clazz.getName(), EntityCacheUtils.INFO_CODE__CACHE_HIT_PART, rtnList.size(), OM3.writeValueAsString(rtnList)));
            pjp.getArgs()[entityCacheable.pKeyIdx()] = argList;
            rtn = pjp.proceed(pjp.getArgs());
          } else {
            if (entityCacheable.pKeyIdx() > -1) {
              log.warn(MessageFormat.format("{0} - {1}", clazz.getName(), EntityCacheUtils.INFO_CODE__CACHE_HIT_MISS));
            }
            rtn = pjp.proceed();
          }
          if (rtn != null) {
            if (rtn instanceof List) {
              List<Object> rstList = (List<Object>) rtn;
              if (rstList.size() > 0) {
                entityCacheAbstractWrapper.hmset(clazz.getName(), rstList.parallelStream().collect(HashMap::new, (a, o) -> {
                  String k = String.valueOf(Object0.gs(o, entityCacheable.rKeyPath()));
                  if (!String0.isNullOrEmpty(k)) {
                    a.put(k, OM3.writeValueAsString(o));
                  }
                }, HashMap::putAll));
              }
              if (rtnList != null) {
                rtnList.addAll(rstList);
              } else {
                rtnList = rstList;
              }
              rtn = rtnList;
            } else {
              String k = String.valueOf(Object0.gs(rtn, entityCacheable.rKeyPath()));
              if (!String0.isNullOrEmpty(k)) {
                entityCacheAbstractWrapper.hset(clazz.getName(), k, OM3.writeValueAsString(rtn));
              }
            }
          } else {
            rtn = rtnList;
          }
        }
      }
    } else {
      log.warn(MessageFormat.format("{0} - {1}", pjp.getSignature().getName(), EntityCacheUtils.ERR_CODE__ANNOTATION_SETTING_ERROR));
      rtn = pjp.proceed();
    }
    return rtn;
  }
}
