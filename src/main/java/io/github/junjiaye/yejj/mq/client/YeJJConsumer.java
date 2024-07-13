package io.github.junjiaye.yejj.mq.client;

import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: yejjmq
 * @ClassName: YeJJConsumer
 * @description:
 * @author: yejj
 * @create: 2024-07-13 08:31
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class YeJJConsumer<T> {

    String id;

    YeJJBroker broker;

    String topic;

    YeJJMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public YeJJConsumer(YeJJBroker broker) {
        this.broker = broker;
        this.id = "CID" +  idgen.getAndIncrement();
    }
    public void subscribe(String topic){
        this.topic = topic;
        mq = broker.find(topic);
        if (mq == null){
            throw new RuntimeException("topic not found");
        }
    }
    public YeJJMessage<T> poll(long timeout){
        return mq.poll(timeout);
    }

    public void listen(YeJJListener<T> listener){
        mq.addListener(listener);
    }

}
