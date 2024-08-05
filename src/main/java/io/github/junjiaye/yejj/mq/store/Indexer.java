package io.github.junjiaye.yejj.mq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: yejjmq
 * @ClassName: Indexer
 * @description:
 * @author: yejj
 * @create: 2024-08-05 14:53
 */
public class Indexer {
    //一个key可以对应多个value
    static MultiValueMap<String,Entry> indexs = new LinkedMultiValueMap<>();

    static Map<Integer,Entry> mappings = new HashMap<>();

    @Data
    @AllArgsConstructor
    public static class Entry{
        int offset;
        int length;
    }

    public static void addEntry(String topic ,int offset,int length){
        Entry entry = new Entry(offset, length);
        mappings.put(offset,entry);
        indexs.add(topic, entry);
    }
    public static List<Entry> getEntry(String topic){
        return indexs.get(topic);
    }

    public static Entry getEntry(String topic,Integer offset){
            return mappings.get(offset);
    }
}
