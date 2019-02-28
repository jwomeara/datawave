package datawave.microservice.audit.replay;

import datawave.microservice.cached.CacheInspector;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;

import java.util.Date;
import java.util.List;

import static datawave.microservice.audit.replay.ReplayStatusCache.CACHE_NAME;

@CacheConfig(cacheNames = CACHE_NAME)
public class ReplayStatusCache {
    public static final String CACHE_NAME = "auditReplay";
    
    private final CacheInspector cacheInspector;
    
    public ReplayStatusCache(CacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    @CachePut(key = "#id")
    public ReplayStatus create(String id, String path, String hdfsUri, long sendRate) {
        ReplayStatus status = new ReplayStatus();
        status.setId(id);
        status.setState(ReplayStatus.ReplayState.CREATED);
        status.setPath(path);
        status.setHdfsUri(hdfsUri);
        status.setSendRate(sendRate);
        status.setLastUpdated(new Date());
        return status;
    }
    
    public ReplayStatus retrieve(String id) {
        return cacheInspector.list(CACHE_NAME, ReplayStatus.class, id);
    }
    
    public List<ReplayStatus> retrieveAll() {
        return (List<ReplayStatus>) cacheInspector.listAll(CACHE_NAME, ReplayStatus.class);
    }
    
    @CachePut(key = "#status.getId()")
    public ReplayStatus update(ReplayStatus status) {
        return status;
    }
    
    @CacheEvict(key = "#id")
    public String delete(String id) {
        return "Evicted " + id;
    }
    
    @CacheEvict(allEntries = true)
    public String deleteAll() {
        return "Evicted all entries";
    }
    
}
