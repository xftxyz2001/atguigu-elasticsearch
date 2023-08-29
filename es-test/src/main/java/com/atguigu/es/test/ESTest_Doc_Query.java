package com.atguigu.es.test;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class ESTest_Doc_Query {
    public static void main(String[] args) throws Exception {

        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

        // 1. 查询索引中全部的数据
        System.out.println("查询索引中全部的数据");
        getAll(esClient);

        // 2. 条件查询: termQuery
        System.out.println("条件查询: termQuery");
        conditionalQuery(esClient);

        // 3. 分页查询
        System.out.println("分页查询");
        pageQuery(esClient);

        // 4. 查询排序
        System.out.println("查询排序");
        sortedQuery(esClient);

        // 5. 过滤字段
        System.out.println("过滤字段");
        fieldsFilter(esClient);

        // 6. 组合查询
        System.out.println("组合查询");
        combinationQuery(esClient);

        // 7. 范围查询
        System.out.println("范围查询");
        rangeQuery(esClient);

        // 8. 模糊查询
        System.out.println("模糊查询");
        fuzzySearch(esClient);

        // 9. 高亮查询
        System.out.println("高亮查询");
        highlightSearch(esClient);

        // 10. 聚合查询
        System.out.println("聚合查询");
        aggregateQuery(esClient);

        // 11. 分组查询
        System.out.println("分组查询");
        groupingQuery(esClient);

        esClient.close();
    }

    private static void groupingQuery(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("ageGroup").field("age");
        builder.aggregation(aggregationBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void aggregateQuery(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        builder.aggregation(aggregationBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void highlightSearch(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name",
                "zhangsan");

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");

        builder.highlighter(highlightBuilder);
        builder.query(termsQueryBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void fuzzySearch(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.fuzzyQuery("name",
                "wangwu").fuzziness(Fuzziness.TWO));

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void rangeQuery(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");

        rangeQuery.gte(30);
        rangeQuery.lt(50);

        builder.query(rangeQuery);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void combinationQuery(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // boolQueryBuilder.must(QueryBuilders.matchQuery("age", 30));
        // boolQueryBuilder.must(QueryBuilders.matchQuery("sex", "男"));
        // boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex", "男"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 30));
        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 40));

        builder.query(boolQueryBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void fieldsFilter(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //
        String[] excludes = { "age" };
        String[] includes = {};
        builder.fetchSource(includes, excludes);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void sortedQuery(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //
        builder.sort("age", SortOrder.DESC);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void pageQuery(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        // (当前页码-1)*每页显示数据条数
        builder.from(2);
        builder.size(2);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void conditionalQuery(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age",
                30)));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private static void getAll(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");

        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.print("命中：" + hits.getTotalHits() + "\t");
        System.out.println("耗时：" + response.getTook());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
}
