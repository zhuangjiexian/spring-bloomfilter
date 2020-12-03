package com.elndu.single;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.util.Assert;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * [简要描述]:
 * [详细描述]:
 *
 * @author elndu
 * @version 1.0, 2020/11/30 10:44
 * @since JDK 1.8
 */
@Slf4j
@Data
public class RedisSingleBloomFilter
{
    //预估元素量
    private long expectedInsertions;
    //误判率
    private double fpp;
    //哈希函数个数
    private int numHashFun;
    //bitmap长度
    private long bitmapLength;

    private RedisTemplate<String, String> redisTemplate;

    public RedisSingleBloomFilter(long expectedInsertions, double fpp, RedisTemplate<String,String> redisTemplate)
    {
        Assert.state(expectedInsertions > 0, "预估元素必须大于0");
        Assert.state(fpp > 0.0D, "误判率必须必须大于0.0D小于1.0D");
        Assert.state(fpp < 1.0D, "误判率必须必须大于0.0D小于1.0D");
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
        this.bitmapLength = optimalNumOfBits(expectedInsertions, fpp);
        this.numHashFun = optimalNumOfHashFunctions(expectedInsertions, bitmapLength);
        this.redisTemplate = redisTemplate;
    }

    /**
     * 计算位数
     */
    private long optimalNumOfBits(long n, double p)
    {
        if (p == 0.0D)
        {
            p = 4.9E-324D;
        }
        return (long) ((double) (-n) * Math.log(p) / (Math.log(2.0D) * Math.log(2.0D)));
    }

    /**
     * 计算哈希函数数量
     */
    private int optimalNumOfHashFunctions(long n, long m)
    {
        return Math.max(1, (int) Math.round((double) m / (double) n * Math.log(2.0D)));
    }

    /**
     * 元素bitmap位数值
     */
    private long[] getBitIndices(String element)
    {
        long[] indices = new long[numHashFun];
        long hash64 = Hashing.murmur3_128().hashObject(element, Funnels.stringFunnel(Charset.defaultCharset())).asLong();
        long hashOne = hash64;
        long hashTwo = hash64 >>> 32;
        for (int i = 1; i <= numHashFun; i++) {
            long nextHash = hashOne + i * hashTwo;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            indices[i - 1] = nextHash % bitmapLength;
        }
        return indices;
    }


    private long[] getBitIndices2(String element){
        long[] indices = new long[numHashFun];
        byte[] bytes = Hashing.murmur3_128().hashObject(element, Funnels.stringFunnel(Charset.defaultCharset()))
                .asBytes();
        long hash1 = lowerEight(bytes);
        long hash2 = upperEight(bytes);
        long combinedHash = hash1;
        for (int i = 0; i < numHashFun; i++)
        {
            indices[i] = (combinedHash & Long.MAX_VALUE) % bitmapLength;
            combinedHash += hash2;
        }
        return indices;
    }

    private long lowerEight(byte[] bytes)
    {
        return Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
    }

    private long upperEight(byte[] bytes)
    {
        return Longs.fromBytes(bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
    }

    /**
     * 加入元素
     *
     * @param key
     * @param ele
     * @param expireSec
     */
    public void set(String key, String ele, int expireSec)
    {
        Assert.state(null != key, "key 不能为空");
        Assert.state(null != ele, "ele 不能为空");
        redisTemplate.executePipelined(new SessionCallback<Object>()
        {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> redisOperations) throws DataAccessException
            {
                for (long index : getBitIndices(ele))
                {
                    redisTemplate.opsForValue().setBit(key, index, true);
                }
                return null;
            }
        });
        redisTemplate.expire(key, expireSec, TimeUnit.MILLISECONDS);

    }

    /**
     *
     * @param key
     * @param ele
     * @return
     */
    public Boolean contains(String key, String ele)
    {
        Assert.state(null != key, "key 不能为空");
        Assert.state(null != ele, "ele 不能为空");
        List<Object> bits = redisTemplate.executePipelined(new SessionCallback<Object>()
        {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> redisOperations) throws DataAccessException
            {
                for (long index : getBitIndices(ele))
                {
                    redisTemplate.opsForValue().getBit(key, index);
                }
                return null;
            }
        });

        return !bits.contains(false);
    }

}
