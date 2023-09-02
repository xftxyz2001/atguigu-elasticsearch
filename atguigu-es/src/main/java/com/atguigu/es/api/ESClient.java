package com.atguigu.es.api;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ESClient {

    private static ElasticsearchTransport transport;

    private static ElasticsearchClient client;

    private static ElasticsearchAsyncClient asyncClient;

    private static final String INDEX_ATGUIGU = "atguigu";

    public static void main(String[] args) throws Exception {
        // 初始化ES服务器的链接
        initESConnection();

        // 操作索引
        // operationIndex();

        // 操作索引，采用lambda表达式
        operationIndexLambda();

        // 操作文档
        // operationDocument();

        // 操作文档，采用lambda表达式
        operationDocumentLambda();

        // 查询文档
        // queryDocument();

        // 查询文档，采用lambda表达式
        queryDocumentLambda();

        // 异步操作
        asyncClientOperation();

    }

    private static void asyncClientOperation() throws Exception {
        asyncClient.indices().create(req -> req.index("newindex"))
                .thenApply(resp -> resp.acknowledged()) // thenApply()方法是在上一个操作完成后执行的
                .whenComplete((resp, err) -> {
                    System.out.println("回调方法");
                    if (resp != null) {
                        System.out.println(resp); // resp是上一个操作的结果
                    } else {
                        err.printStackTrace();
                    }
                });
        System.out.println("主线程代码执行完毕");
    }

    private static void queryDocumentLambda() throws Exception {
        client.search(req -> {
            req.query(q -> q.match(m -> m.field("name").query("zhangsan")));
            return req;
        }, Object.class).hits();

        // MatchQuery matchQuery = new
        // MatchQuery.Builder().field("age").query(30).build();
        // Query query = new Query.Builder().match(matchQuery).build();
        // SearchRequest searchRequest = new
        // SearchRequest.Builder().query(query).build();
        // final SearchResponse<Object> search = client.search(searchRequest,
        // Object.class);
        // System.out.println("查询的响应结果 :" + search);
    }

    private static void queryDocument() throws Exception {
        MatchQuery matchQuery = new MatchQuery.Builder().field("age").query(30).build();
        Query query = new Query.Builder().match(matchQuery).build();
        SearchRequest searchRequest = new SearchRequest.Builder().query(query).build();
        final SearchResponse<Object> search = client.search(searchRequest, Object.class);
        System.out.println("查询的响应结果 :" + search);
    }

    private static void operationDocumentLambda() throws Exception {

        User user = new User();
        user.setId(1001);
        user.setName("zhangsan");
        user.setAge(30);

        // 增加文档
        Result create = client.create(req -> req.index(INDEX_ATGUIGU)
                .id("1001")
                .document(user)).result();
        System.out.println("创建文档的响应对象 :" + create);

        // 批量添加数据
        List<User> users = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            users.add(new User(3000 + i, "lisi" + i, 20 + i));
        }
        client.bulk(req -> {
            users.forEach(
                    u -> req.operations(
                            b -> b.create(
                                    d -> d.index(INDEX_ATGUIGU)
                                            .id(u.getId().toString())
                                            .document(u))));
            return req;
        });

        // 文档的删除
        client.delete(req -> req.index(INDEX_ATGUIGU).id("3001"));

        transport.close();
    }

    private static void operationDocument() throws Exception {

        User user = new User();
        user.setId(1001);
        user.setName("zhangsan");
        user.setAge(30);

        CreateRequest<User> createRequest = new CreateRequest.Builder<User>()
                .index(INDEX_ATGUIGU)
                .id("1001")
                .document(user)
                .build();

        // 增加文档
        // final CreateResponse createResponse = client.create(createRequest);
        // System.out.println("创建文档的响应对象 :" + createResponse);

        // 批量添加数据
        List<BulkOperation> opts = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            CreateOperation<User> optObj = new CreateOperation.Builder<User>()
                    .index(INDEX_ATGUIGU)
                    .id("200" + i)
                    .document(new User(i, "lisi" + i, 20 + i))
                    .build();
            BulkOperation opt = new BulkOperation.Builder()
                    .create(optObj).build();
            opts.add(opt);
        }
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(opts).build();
        final BulkResponse bulk = client.bulk(bulkRequest);
        System.out.println("批量添加数据的响应对象 :" + bulk);

        // 文档的删除
        DeleteRequest deleteRequest = new DeleteRequest.Builder()
                .index(INDEX_ATGUIGU)
                .id("2001")
                .build();
        final DeleteResponse delete = client.delete(deleteRequest);
        System.out.println(delete);

        transport.close();
    }

    private static void operationIndexLambda() throws Exception {
        // 获取索引客户端对象
        final ElasticsearchIndicesClient indices = client.indices();

        // 判断索引是否存在
        final boolean flg = indices.exists(req -> req.index(INDEX_ATGUIGU)).value();
        if (flg) {
            System.out.println("索引" + INDEX_ATGUIGU + "已经存在");
        } else {
            // 创建索引
            // 需要采用构建器方式来构建对象，ESAPI的对象基本上都是采用这种方式
            CreateIndexResponse createIndexResponse = indices.create(req -> req.index(INDEX_ATGUIGU));
            System.out.println("创建索引响应对象 =" + createIndexResponse);
        }

        // 查询索引
        final IndexState atguigu = indices.get(req -> req.index(INDEX_ATGUIGU)).get(INDEX_ATGUIGU);

        // atguigu.xxx
        // System.out.println("查询的响应结果 :" + getIndexResponse);

        // 删除索引
        final DeleteIndexResponse deleteIndexResponse = indices.delete(req -> req.index(INDEX_ATGUIGU));
        System.out.println("索引删除的响应结果 :" + deleteIndexResponse.acknowledged());

        transport.close();

    }

    private static void operationIndex() throws Exception {
        // 获取索引客户端对象
        final ElasticsearchIndicesClient indices = client.indices();

        // 判断索引是否存在
        ExistsRequest existsRequest = new ExistsRequest.Builder().index(INDEX_ATGUIGU).build();
        final boolean flg = indices.exists(existsRequest).value();
        if (flg) {
            System.out.println("索引" + INDEX_ATGUIGU + "已经存在");
        } else {
            // 创建索引
            // 需要采用构建器方式来构建对象，ESAPI的对象基本上都是采用这种方式
            CreateIndexRequest request = new CreateIndexRequest.Builder().index(INDEX_ATGUIGU).build();
            CreateIndexResponse createIndexResponse = indices.create(request);
            System.out.println("创建索引响应对象 =" + createIndexResponse);
        }

        // 查询索引
        GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index(INDEX_ATGUIGU).build();
        final GetIndexResponse getIndexResponse = indices.get(getIndexRequest);
        final IndexState atguigu = getIndexResponse.get(INDEX_ATGUIGU);
        // atguigu.xxx
        System.out.println("查询的响应结果 :" + getIndexResponse);

        // 删除索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index(INDEX_ATGUIGU).build();
        final DeleteIndexResponse deleteIndexResponse = indices.delete(deleteIndexRequest);
        System.out.println("索引删除的响应结果 :" + deleteIndexResponse.acknowledged());

        transport.close();

    }

    private static void initESConnection() throws Exception {
        // 获取客户端对象
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                // 这里的用户名和密码是es的用户名和密码
                new UsernamePasswordCredentials("elastic", "O3x0hfu7i=ZbQvlktCnd"));
        Path caCertificatePath = Paths.get("certs/java-ca.crt");
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();
        RestClientBuilder builder = RestClient.builder(
                // ES服务器的地址和端口号
                new HttpHost("linux1", 9200, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext)
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestClient restClient = builder.build(); // 创建rest客户端
        transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()); // 创建传输对象
        // 创建同步客户端对象
        client = new ElasticsearchClient(transport);
        // 创建异步客户端对象
        asyncClient = new ElasticsearchAsyncClient(transport);

        // do something...

        // transport.close(); // 对于同步客户端，需要关闭传输对象

    }
}
