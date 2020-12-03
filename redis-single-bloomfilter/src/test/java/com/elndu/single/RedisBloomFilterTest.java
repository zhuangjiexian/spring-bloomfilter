package com.elndu.single;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author elndu
 * @version 1.0, 2020/11/30 18:58
 * @since JDK 1.8
 */
@SpringBootTest
public class RedisBloomFilterTest
{

    @Autowired
    private RedisTemplate<String,String> redisTemplate;

    @Test
    public void testBloomFilter()
    {
        RedisSingleBloomFilter singleBloomFilter = new RedisSingleBloomFilter(10000,0.01,redisTemplate);
        singleBloomFilter.set("bloom-filter","13914383246",1000000);
        singleBloomFilter.set("bloom-filter","13814373287",1000000);
        singleBloomFilter.set("bloom-filter","13814373288",1000000);
        singleBloomFilter.set("bloom-filter","13814373289",1000000);
        singleBloomFilter.set("bloom-filter","13814373280",1000000);

        System.out.println(singleBloomFilter.contains("bloom-filter","13914383246"));
        System.out.println(singleBloomFilter.contains("bloom-filter","7777777777"));
        System.out.println(singleBloomFilter.contains("bloom-filter","13814373289"));
        System.out.println(singleBloomFilter.contains("bloom-filter","13814387299"));
    }
}
