package io.github.junjiaye.yejj.mq.server;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @program: yejjmq
 * @ClassName: MessageSubscription
 * @description: 订阅关系
 * @author: yejj
 * @create: 2024-07-13 11:35
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageSubscription {
    private String topic;
    private String consumerId;
    //消费消息的游标
    private int offset = -1;
}
