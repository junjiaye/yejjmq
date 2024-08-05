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

    //String topic;


    static AtomicInteger idgen = new AtomicInteger(0);

    public YeJJConsumer(YeJJBroker broker) {
        this.broker = broker;
        this.id = "CID" +  idgen.getAndIncrement();
    }
    public void sub(String topic){
        broker.sub(topic,id);

    }
    public void unsub(String topic){
        broker.unsub(topic,id);

    }
    public YeJJMessage<T> recv(String topic){
        return broker.recv(topic,id);
    }
    public boolean ack(String topic, int offset){
        return broker.ack(topic,id,offset);
    }
    public boolean ack(String topic, YeJJMessage<?> message){
        if (message.getHeaders().isEmpty()){
            return false;
        }
        String offset = message.getHeaders().get("X-offset");
        return broker.ack(topic,id,Integer.valueOf(offset));
    }

    public void listen(String topic,YeJJListener<T> listener){
        broker.addListener(topic,listener);
    }

}
