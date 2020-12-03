package com.elndu.guava.init;


import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@Component
@Slf4j
public class InitBloomFilterConstruct {

    //测试数据量
    @Value("${bloom.filter.expectedInsertions}")
    private int expectedInsertions;
    //误判率
    @Value("${bloom.filter.fpp}")
    private float fpp;
    @Value("${bloom.filter.record}")
    private int record;
    @Value("${bloom.filter.testData}")
    private int testData;

    private HashMap<String, Boolean> recordMap = new HashMap<>();
    private List<String> recordList = new ArrayList<>();

    @PostConstruct
    public void initBloomFilter() {
        //实例化
        BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), expectedInsertions, fpp);
        //添加数据集
        for (int i = 0; i < expectedInsertions ; i++) {
            String uuid = UUID.randomUUID().toString();
            String bfData = uuid;
            bloomFilter.put(bfData);
            recordList.add(bfData);
            recordMap.put(bfData, true);
        }

        int right = 0;
        int wrong = 0;

        for (int i = 0; i < testData; i++) {
            String data = i < record  ? recordList.get(i) : UUID.randomUUID().toString();
            if (bloomFilter.mightContain(data)) {
                if (recordMap.get(data) != null) {
                    right++;
                    continue;
                }
                wrong++;
            }

        }

        NumberFormat numberFormat = NumberFormat.getPercentInstance();
        numberFormat.setMaximumFractionDigits(2);
        float percent = (float) wrong / (testData - record);
        float bingo = (float) (testData - record - wrong) / (testData - record);
        log.info("预估元素数：" + expectedInsertions);
        log.info("判断" + record + "条存在的数据,布隆过滤器认为存在：" + right);
        log.info("判断" + (testData - record) + "条不存在的数据，布隆过滤器认为存在：" + wrong);
        log.info("命中率为：" + numberFormat.format(bingo) + "，误判率为：" + numberFormat.format(percent));

    }
}
