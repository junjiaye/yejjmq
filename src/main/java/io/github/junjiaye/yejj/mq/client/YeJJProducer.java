package io.github.junjiaye.yejj.mq.client;

import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import lombok.AllArgsConstructor;

/**
 * @program: yejjmq
 * @ClassName: YeJJProducer
 * @description:
 * @author: yejj
 * @create: 2024-07-13 08:16
 */
@AllArgsConstructor
public class YeJJProducer {

    YeJJBroker broker;

    public boolean send(String topic, YeJJMessage message){
        return broker.send(topic,message);

    }
}
