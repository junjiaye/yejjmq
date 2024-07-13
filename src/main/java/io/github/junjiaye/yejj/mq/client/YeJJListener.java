package io.github.junjiaye.yejj.mq.client;

import io.github.junjiaye.yejj.mq.model.YeJJMessage;

/**
 * @program: yejjmq
 * @ClassName: YeJJListener
 * @description:
 * @author: yejj
 * @create: 2024-07-13 09:50
 */
public interface YeJJListener<T> {
    void onMessage(YeJJMessage<T> message);
}
