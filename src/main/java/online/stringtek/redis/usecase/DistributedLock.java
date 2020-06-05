package online.stringtek.redis.usecase;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.HashMap;
import java.util.Map;

/**
 * 分布式锁--可重入锁
 * 暂时没有对redis锁过期的问题做处理
 * */

@Component
public class DistributedLock {
    private final ThreadLocal<Map<String,Integer>> currentLockers=new ThreadLocal<>();

    private Jedis jedis;
    public DistributedLock(Jedis jedis){
        this.jedis=jedis;
    }
    private boolean _lock(String key){
        //使用原子指令，同时设置值和过期时间
        return jedis.set(key,"",new SetParams().nx().ex(5))!=null;
    }
    public void _unlock(String key){
        jedis.del(key);
    }
    private Map<String,Integer> currentLockers(){
        Map<String,Integer> map=currentLockers.get();
        if(map!=null)
            return map;
        currentLockers.set(new HashMap<>());
        return currentLockers.get();
    }
    public boolean lock(String key){
        Map<String,Integer> ref=currentLockers();
        Integer cnt=ref.get(key);
        if(cnt!=null){
            //说明当前线程已经获取到了锁，他想重入
            ref.put(key,cnt+1);
            return true;
        }
        boolean ok=_lock(key);
        if(!ok){
            //抢锁失败
            return false;
        }
        //枪锁成功
        ref.put(key,1);
        return true;
    }
    public boolean unlock(String key){
        Map<String,Integer> ref=currentLockers();
        Integer cnt=ref.get(key);
        if(cnt==null){
            //没有获取到锁，更不用说解锁
            return false;
        }
        cnt--;
        if(cnt==0){
            //此时可以remove
            ref.remove(key);
            _unlock(key);
        }else{
            ref.put(key,cnt);
        }
        return true;
    }
}
