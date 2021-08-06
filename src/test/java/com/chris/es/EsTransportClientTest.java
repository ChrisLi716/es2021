package com.chris.es;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.json.JSONUtil;
import com.chris.es.entity.Book;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * @Author Lilun
 * @Date 2021-08-05 19:53
 * @Description
 **/
public class EsTransportClientTest {

    private TransportClient transportClient;

    @Before
    public void init() throws UnknownHostException {
        transportClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("master"), 9300));
    }

    @Test
    public void testAddIndex() {
        Map<String, Object> doc = CollUtil.newHashMap();
        doc.put("name", "小黑的故事");
        doc.put("sex", "男");
        doc.put("age", 25);
        doc.put("content", "公安部：各地校车将享最高路权");

        //自动生成_id
        transportClient.prepareIndex("dangdang", "book", "12121")
                .setSource(doc)
                .get();

        IndexResponse response = transportClient.prepareIndex("dangdang", "book")
                .setSource(doc) //设置文档数据
                .get(); //发送请求到ES

        System.out.println(response);
    }

    @Test
    public void testAddIndexJson() {
        Book doc = new Book();
        doc.setId(UUID.randomUUID().toString()).setName("小黑娃的故事").setSex("男").setAge(27).setContent("批量索引修改刷新时间配置");

        IndexResponse response = transportClient.prepareIndex("dangdang", "book", doc.getId())
                .setSource(JSONUtil.toJsonStr(doc), XContentType.JSON)
                .get();


        System.out.println(response);
    }

    @Test
    public void testUpdateIndex() {
        Map<String, Object> doc = CollUtil.newHashMap();
        doc.put("name", "小黑的故事-2");

        UpdateResponse response = transportClient.prepareUpdate("dangdang", "book", "12121")
                .setDoc(doc)
                .get();

        System.out.println(response);
    }

    @Test
    public void testUpdateIndexJson() {
        Book doc = new Book();
        doc.setId("VA3jGXsBCLbw_XwYpQfv").setName("月亮").setSex("男").setAge(2).setContent("本例使用了 Github 作为远程仓库,你可以先阅读我们的Github 简明教程。 添加远程库 要添加一个新的远程仓库,可以指定一个简单的名字,以便将来引用,命令格式如下");

        UpdateResponse response = transportClient.prepareUpdate("dangdang", "book", doc.getId())
                .setDoc(JSONUtil.toJsonStr(doc), XContentType.JSON)
                .get();

        System.out.println(response);
    }

    @Test
    public void testGetOne() {
        GetResponse response = transportClient.prepareGet("dangdang", "book", "5b792ae0-5918-423c-8d7a-69b2d321d5c5").get();
        Book book = JSONUtil.toBean(response.getSourceAsString(), Book.class);
        System.out.println(book.toString());
    }


    @Test
    public void testDeleteOne() {
        DeleteResponse response = transportClient.prepareDelete("dangdang", "book", "12121").get();
        System.out.println(response.status());
    }

    @Test
    public void testBulk() {
        Book doc = new Book();
        doc.setId("110").setName("太阳的故事").setSex("男").setAge(2).setContent("全红婵妹妹的故事好像一部武侠小说！在操场跳格子被教练发掘，为了给妈妈治病全力以赴");

        IndexRequest index = new IndexRequest("dangdang").id(doc.getId()).source(JSONUtil.toJsonStr(doc), XContentType.JSON);

        Book updateDoc = new Book();
        updateDoc.setId("UQ04FnsBCLbw_XwYlAeq").setAge(43);
        UpdateRequest update = new UpdateRequest("dangdang", updateDoc.getId()).doc(JSONUtil.toJsonStr(updateDoc), XContentType.JSON);

        DeleteRequest delete = new DeleteRequest("dangdang", "5b792ae0-5918-423c-8d7a-69b2d321d5c5");

        BulkResponse bulkItemResponses = transportClient.prepareBulk().add(index).add(update).add(delete).get();
        System.out.println(JSONUtil.toJsonPrettyStr(bulkItemResponses));

        for (BulkItemResponse item : bulkItemResponses.getItems()) {
            System.out.println(JSONUtil.toJsonPrettyStr(item));
        }
    }

    @Test
    public void testSearch1() {
        SearchResponse response = transportClient.prepareSearch("dangdang")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(2) //每次返回记录灵敏
                .setFrom(1) //从第几页开始
                .get();
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        if (0 < totalHits.value) {
            for (SearchHit hit : hits.getHits()) {
                System.out.println(hit.getIndex());
                System.out.println(hit.getId());
                System.out.println(hit.getScore());
                System.out.println(hit.getSourceAsMap().get("name"));
                System.out.println(hit.getSourceAsMap().get("content"));
            }
        }

    }

    @Test
    public void testSearch2() throws ExecutionException, InterruptedException {
        SearchRequest searchRequest = new SearchRequest("dangdang");
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        searchRequest.source(query);
        SearchResponse response = transportClient.search(searchRequest).get();
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        if (0 < totalHits.value) {
            for (SearchHit hit : hits.getHits()) {
                System.out.println(hit.getIndex());
                System.out.println(hit.getId());
                System.out.println(hit.getScore());
                System.out.println(hit.getSourceAsMap().get("name"));
                System.out.println(hit.getSourceAsMap().get("content"));
            }
        }
    }


    @After
    public void close() {
        if (Objects.nonNull(transportClient)) {
            transportClient.close();
        }
    }

}
