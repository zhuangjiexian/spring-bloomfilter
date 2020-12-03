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
 * @version 1.0, 2020/11/30 14:14
 * @since JDK 1.8
 */
@SpringBootTest
@Slf4j
public class RedisConnectTest
{
    @Autowired
    private RedisTemplate<String,String> redisTemplate;

    @Test
    public void testRedisConnect(){
        redisTemplate.opsForValue().set("hello","world");
        log.info("hello:{}",redisTemplate.opsForValue().get("hello"));
    }
}
