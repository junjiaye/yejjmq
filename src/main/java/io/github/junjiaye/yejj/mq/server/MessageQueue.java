package io.github.junjiaye.yejj.mq.server;

import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import io.github.junjiaye.yejj.mq.store.Indexer;
import io.github.junjiaye.yejj.mq.store.Store;

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
    //private YeJJMessage<?>[] queue = new YeJJMessage[1024 * 10];
    private Store store = null;
    //队列当前位置的游标
//    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
        store = new Store(topic);
        store.init();
    }
    public int send(YeJJMessage<?> message){
        int offset = store.pos();
        message.getHeaders().put("X-offset",String.valueOf(offset));
        store.write((YeJJMessage<String>) message);
        return offset;
    }

    public YeJJMessage<?> recv(int offset){
        return store.read(offset);
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

    public static int send(String topic, YeJJMessage<String> message){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }
    public static YeJJMessage<?> recv(String topic, String comsumerId,
                                      int offset){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptions.containsKey(comsumerId)){
            throw  new RuntimeException("subscription not found for topic/comsumerId = " + topic + "/" + comsumerId);
        }
        return messageQueue.recv(offset);
    }
    //使用此方法，需要手动调用ack，更新订阅关系中的offset
    public static YeJJMessage<?> recv(String topic, String comsumerId){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptions.containsKey(comsumerId)){
            throw  new RuntimeException("subscription not found for topic/comsumerId = " + topic + "/" + comsumerId);
        }
        int offset = messageQueue.subscriptions.get(comsumerId).getOffset();
        int next_offset = 0;
        if (offset > -1 ){
            Indexer.Entry entry = Indexer.getEntry(topic, offset);
            next_offset = entry.getLength() + offset;
        }
        return messageQueue.recv(next_offset);
    }

    public static int ack(String topic, String comsumerId,int offset){
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptions.containsKey(comsumerId)){
            throw  new RuntimeException("subscription not found for topic/comsumerId = " + topic + "/" + comsumerId);
        }
        MessageSubscription subscription = messageQueue.subscriptions.get(comsumerId);
        if (offset > subscription.getOffset() && offset <= Store.SIZE){
            subscription.setOffset(offset);
            return offset;
        }
        return -1;
    }


}
