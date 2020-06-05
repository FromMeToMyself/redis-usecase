package online.stringtek.redis.usecase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import javafx.concurrent.Task;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.UUID;

/**
 * 延时队列
 * */
public class DelayQueue<T> {
    private Jedis jedis;
    private String key;
    private Type taskType=new TypeReference<TaskItem<T>>(){}.getType();
    public DelayQueue(Jedis jedis,String key){
        this.jedis=jedis;
        this.key=key;
    }
    private static class TaskItem<T>{
        public String id;
        public T msg;
    }

    public void enqueue(T msg){
        TaskItem<T> taskItem=new TaskItem<>();
        taskItem.id= UUID.randomUUID().toString();
        taskItem.msg=msg;
        String json=JSON.toJSONString(taskItem);
        //放入延时队列延时1s处理
        jedis.zadd(key,System.currentTimeMillis()+1000,json);
    }
    public void loop(){
        while(!Thread.interrupted()){
            //取一个任务处理
            Set<String> items=jedis.zrangeByScore(key,0,System.currentTimeMillis(),0,1);
            if(items.isEmpty()){
                //当前队列中无任务
                try{
                    Thread.sleep(500);
                }catch (InterruptedException e){
                    //传递interrupt 也可以直接break
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            String value=items.iterator().next();
            //这之间可能有多个线程拿到这个item,可以通过lua脚本来实现一个原子操作来将获取和remove的过程合并在一起
            if(jedis.zrem(key,value)>1){
                try{
                    TaskItem<T> taskItem=JSON.parseObject(value,taskType);
                    handleMsg(taskItem.msg);
                }catch (Exception e){
                    //这里catch住是为了避免一个任务有问题从而影响别的任务
                    e.printStackTrace();
                }
            }
        }
    }
    private void handleMsg(T msg){
        System.out.println(msg);
    }

}
