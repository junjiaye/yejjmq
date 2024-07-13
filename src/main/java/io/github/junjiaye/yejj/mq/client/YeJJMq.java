package io.github.junjiaye.yejj.mq.client;

import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @program: yejjmq
 * @ClassName: YeJJMq
 * @description:
 * @author: yejj
 * @create: 2024-07-13 08:20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class YeJJMq {
    private String topic;

    private List<YeJJListener> listeners = new ArrayList<>();

    private LinkedBlockingQueue<YeJJMessage> queue = new LinkedBlockingQueue<>();


    public YeJJMq(String topic) {
        this.topic = topic;
    }
    public boolean send(YeJJMessage message) {
        boolean offer = queue.offer(message);
        listeners.forEach( listener -> listener.onMessage(message));
        return offer;
    }
    //拉取消息
    @SneakyThrows
    public <T> YeJJMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(YeJJListener<T> listener) {
        listeners.add(listener);
    }
}
