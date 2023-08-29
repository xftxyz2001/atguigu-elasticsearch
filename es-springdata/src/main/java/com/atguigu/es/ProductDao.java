package com.atguigu.es;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

// @Repository
public interface ProductDao extends ElasticsearchRepository<Product, Long> {
}
