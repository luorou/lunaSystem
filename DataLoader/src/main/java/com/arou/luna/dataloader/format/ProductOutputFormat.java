package com.arou.luna.dataloader.format;

import com.arou.luna.dataloader.model.Product;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProductOutputFormat implements OutputFormat<Product> {

    static MongoClient mongoClient = null;
    static MongoDatabase mongoDatabase = null;

    @Override
    public void configure(Configuration parameters) {
        System.out.println("mongoSink 配置中");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        System.out.println("连接 mongoDB ");
        // 连接到 mongodb 服务
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        ServerAddress address = new ServerAddress("106.12.145.41", 27017);
        credentials.add(MongoCredential.createCredential("luna", "luna", "luna1748".toCharArray()));
        mongoClient = new MongoClient(address, credentials);
        // 连接到数据库
        mongoDatabase = mongoClient.getDatabase("luna");

        System.out.println("Connect to database successfully");
    }

    @Override
    public void writeRecord(Product products) throws IOException {
        MongoCollection<Document> collection = mongoDatabase.getCollection("Product");
        //插入文档
        Document document = new Document("productId", products.getProductId()).
                append("name", products.getName()).
                append("imageUrl", products.getImageUrl()).
                append("categories", products.getCategories()).
                append("tags", products.getTags());
        collection.insertOne(document);
        System.out.println("文档插入成功");
    }

    @Override
    public void close() throws IOException {
        System.out.println("close mongo connect");
        mongoClient.close();
    }
}
