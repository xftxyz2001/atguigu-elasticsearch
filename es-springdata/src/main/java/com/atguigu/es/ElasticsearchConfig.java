package com.atguigu.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@ConfigurationProperties(prefix = "elasticsearch")
@Configuration
@Data /// fix Generating equals/hashCode implementation but without a call to
/// superclass, even though this class does not extend java.lang.Object. If this
/// is intentional, add '@EqualsAndHashCode(callSuper=false)' to your type.,
/// Generating equals/hashCode implementation but without a call to superclass,
/// even though this class does not extend java.lang.Object. If this is
/// intentional, add '@EqualsAndHashCode(callSuper=false)' to your type.
@EqualsAndHashCode(callSuper = false)
public class ElasticsearchConfig extends AbstractElasticsearchConfiguration {
    private String host;
    private Integer port;

    // 重写父类方法
    @Override
    public RestHighLevelClient elasticsearchClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);
        return restHighLevelClient;
    }
}
