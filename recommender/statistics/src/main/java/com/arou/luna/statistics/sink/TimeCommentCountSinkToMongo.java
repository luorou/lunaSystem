package com.arou.luna.statistics.sink;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class TimeCommentCountSinkToMongo extends RichSinkFunction<Tuple3<Long, String, Long>> {
    static MongoClient mongoClient = null;
    static MongoDatabase mongoDatabase = null;
    static MongoCollection<Document> collection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        buildConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (mongoClient != null) {
            mongoClient.close();
        }
        System.out.println(" 关闭 连接");
    }

    @Override
    public void invoke(Tuple3<Long, String, Long> value, Context context) throws Exception {

        UpdateOptions options = new UpdateOptions().upsert(true);
        collection.updateMany(Filters.and(Filters.eq("productId", value.f0),
                Filters.eq("yearmonth", value.f1)),
                new Document("$set", new Document("productId", value.f0).append("yearmonth", value.f1).append("count", value.f2)),
                options);
        System.out.println(" 插入成功");
    }

    private void buildConnection() {
        // 连接到 mongodb 服务
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        ServerAddress address = new ServerAddress("106.12.145.41", 27017);
        credentials.add(MongoCredential.createCredential("luna", "luna", "luna1748".toCharArray()));
        if (mongoClient == null) {
            mongoClient = new MongoClient(address, credentials);
        }
        if (mongoDatabase == null) {
            mongoDatabase = mongoClient.getDatabase("luna");
        }
//        mongoDatabase.createCollection("RateMoreRecentlyProducts");
        // 连接到数据库
        if (collection == null) {
            collection = mongoDatabase.getCollection("RateMoreRecentlyProducts");
        }
        System.out.println("连接 mongoDB ");
    }
}
