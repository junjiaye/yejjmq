package io.github.junjiaye.yejj.mq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @program: yejjmq
 * @ClassName: Order
 * @description:
 * @author: yejj
 * @create: 2024-07-13 09:35
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private long id;
    private String item;
    private double price;
}
