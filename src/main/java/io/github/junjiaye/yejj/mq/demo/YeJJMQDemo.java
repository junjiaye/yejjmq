package io.github.junjiaye.yejj.mq.demo;

import com.alibaba.fastjson.JSONObject;
import io.github.junjiaye.yejj.mq.client.YeJJBroker;
import io.github.junjiaye.yejj.mq.client.YeJJConsumer;
import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import io.github.junjiaye.yejj.mq.client.YeJJProducer;

import java.io.IOException;

/**
 * @program: yejjmq
 * @ClassName: YeJJMQDemo
 * @description:
 * @author: yejj
 * @create: 2024-07-13 09:34
 */
public class YeJJMQDemo {
    public static void main(String[] args) throws IOException {
        String topic = "cn.yejj.test";
        YeJJBroker broker = YeJJBroker.Default;

        YeJJProducer producer = broker.createProducer();
        YeJJConsumer<?> consumer = broker.createConsumer(topic);

        consumer.listen(topic,message -> {
            System.out.println("onmessage => "  + message);
        });
        for (int i = 0; i < 10; i++) {
            Order order = new Order(i,"item" + i ,100*i);
            producer.send(topic,new YeJJMessage((long)i, JSONObject.toJSONString(order),null));
        }
        for (int i = 0; i < 10; i++) {
            YeJJMessage<String> msg = (YeJJMessage<String>) consumer.recv(topic);
            System.out.println(msg);
            consumer.ack(topic,msg);
        }
    }
}
