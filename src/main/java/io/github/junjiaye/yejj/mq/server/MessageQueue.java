package io.github.junjiaye.yejj.mq.server;

import io.github.junjiaye.yejj.mq.model.YeJJMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: yejjmq
 * @ClassName: MessageQueue
 * @description:
 * @author: yejj
 * @create: 2024-07-13 11:24
 */
public class MessageQueue {
    public static final Map<String,MessageQueue> queues = new HashMap<>();

    public static final String TEST_TOPIC = "cn.yejj.test";

    static {
        queues.put(TEST_TOPIC,new MessageQueue(TEST_TOPIC));
    }
    public Map<String,MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
    //初始化10K。需要增加扩容
    private YeJJMessage<?>[] queue = new YeJJMessage[1024 * 10];
    //队列当前位置的游标
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }
    public int send(YeJJMessage<?> message){
        if (index >= queue.length){
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public YeJJMessage<?> recy(int ind){
        if (ind > index){
            return null;
        }
        return queue[ind];
    }

    public void subscribe(MessageSubscription subscription){
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId,subscription);
    }
    public void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }
    public static void sub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }
    public static void unsub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic,  String comsumerId,
                            YeJJMessage<String> message){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }
    public static YeJJMessage<?> recv(String topic, String comsumerId,
                                      int ind){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptions.containsKey(comsumerId)){
            throw  new RuntimeException("subscription not found for topic/comsumerId = " + topic + "/" + comsumerId);
        }
        return messageQueue.recy(ind);
    }
    //使用此方法，需要手动调用ack，更新订阅关系中的offset
    public static YeJJMessage<?> recv(String topic, String comsumerId){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptions.containsKey(comsumerId)){
            throw  new RuntimeException("subscription not found for topic/comsumerId = " + topic + "/" + comsumerId);
        }
        int ind = messageQueue.subscriptions.get(comsumerId).getOffset();
        return messageQueue.recy(ind);
    }

    public static int ack(String topic, String comsumerId,int offset){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptions.containsKey(comsumerId)){
            throw  new RuntimeException("subscription not found for topic/comsumerId = " + topic + "/" + comsumerId);
        }
        MessageSubscription subscription = messageQueue.subscriptions.get(comsumerId);
        if (offset > subscription.getOffset() && offset <= messageQueue.index){
            subscription.setOffset(offset);
            return offset;
        }
        return -1;
    }
}
