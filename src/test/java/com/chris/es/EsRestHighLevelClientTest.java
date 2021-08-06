package com.chris.es;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * @Author Lilun
 * @Date 2021-08-05 11:32
 * @Description
 **/
public class EsRestHighLevelClientTest {


    private RestHighLevelClient client;

    private final String SCHEME = "HTTP";

    HttpHost[] httpHosts = new HttpHost[]{
            new HttpHost("master", 9200, SCHEME)
    };

    @Before
    public void init() {
        client = new RestHighLevelClient(RestClient.builder(httpHosts));
    }

    @Test
    public void testAddIndex() throws IOException {
        Map<String, Object> map = CollUtil.newHashMap();
        map.put("name", "chris");
        map.put("content", "美国留给伊拉克的是个烂摊子吗");

        //如果不指定ID为自动生成ID
        IndexRequest indexRequest = new IndexRequest("test").id("110").source(map);

        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response);

        Map<String, Object> map2 = CollUtil.newHashMap();
        map2.put("name", "steve");
        map2.put("content", "31个省区市新增本土62+32,涉及8省市");

        IndexRequest indexRequest2 = new IndexRequest("test").id("111").source(JSONUtil.toJsonStr(map2), XContentType.JSON);
        IndexResponse response2 = client.index(indexRequest2, RequestOptions.DEFAULT);
        System.out.println(response2);
    }

    @Test
    public void testUpdateIndex() throws IOException {
        Map<String, Object> map = CollUtil.newHashMap();
        map.put("name", "Petter");
        map.put("content", "xxxx");

        UpdateRequest updateRequest = new UpdateRequest("test", "110").doc(map);
        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    public void testGetIndex() throws IOException {
        //如果指定的索引不存在则报错
        GetRequest getRequest = new GetRequest("test", "110");

        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    public void testDeleteIndex() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test", "110");

        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @After
    public void close() throws IOException {
        if (Objects.nonNull(client)) {
            client.close();
        }
    }

    @Test
    public void testBatch() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        Map<String, String> indexMap = CollUtil.newHashMap();
        indexMap.put("name", "Nancy");
        indexMap.put("content", "Specifying types in search requests is deprecated.");

        Map<String, String> updateMap = CollUtil.newHashMap();
        indexMap.put("name", "Jobs");
        indexMap.put("content", "开发环境地址-DEV3环境");

        bulkRequest.add(new IndexRequest("test").source(JSONUtil.toJsonStr(indexMap), XContentType.JSON))
                .add(new UpdateRequest("test", "111").doc(JSONUtil.toJsonStr(updateMap), XContentType.JSON))
                .add(new DeleteRequest("test", "VQ0cGnsBCLbw_XwYhAdE"));

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse);
        for (BulkItemResponse item : bulkResponse.getItems()) {
            System.out.println(JSONUtil.toJsonPrettyStr(item));
        }
    }


    @Test
    public void batchSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("test");
        //如果结果超过10条默认返回10条数据
        SearchSourceBuilder queryAll = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        searchRequest.source(queryAll);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response);
        long value = response.getHits().getTotalHits().value;
        if (0 < value) {
            for (SearchHit hit : response.getHits().getHits()) {
                System.out.println(hit.getIndex());
                System.out.println(hit.getId());
                System.out.println(hit.getScore());
                System.out.println(hit.getSourceAsMap().get("name"));
                System.out.println(hit.getSourceAsMap().get("content"));
            }
        }
    }
}
