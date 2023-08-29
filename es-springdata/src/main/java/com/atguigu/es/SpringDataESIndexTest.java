package com.atguigu.es;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringDataESIndexTest {
    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    // 创建索引并增加映射配置
    @Test
    public void createIndex() {
        System.out.println("创建索引");
    }

    @Test
    public void deleteIndex() {
        // 创建索引，系统初始化会自动创建索引
        // Deprecated since 4.0, use IndexOperations.delete()
        // boolean flg = elasticsearchRestTemplate.deleteIndex(Product.class);

        boolean flg = elasticsearchRestTemplate.indexOps(Product.class).delete();

        System.out.println("删除索引 = " + flg);
    }
}
