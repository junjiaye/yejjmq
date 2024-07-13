package io.github.junjiaye.yejj.mq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @program: yejjmq
 * @ClassName: YeJJBroker
 * @description:
 * @author: yejj
 * @create: 2024-07-13 08:20
 */
public class YeJJBroker {

    Map<String, YeJJMq> mqMapping = new ConcurrentHashMap<>(64);
    public YeJJMq find(String topic) {
        return mqMapping.get(topic);
    }

    public YeJJMq createTopic(String topic){
        return mqMapping.putIfAbsent(topic,new YeJJMq(topic));
    }

    public  YeJJProducer createProducer(){
        return new YeJJProducer(this);
    }
    public  YeJJConsumer<?> createConsumer(String topic){
        YeJJConsumer<?> consumer = new YeJJConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }
}
