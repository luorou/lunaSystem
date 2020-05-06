package com.arou.luna.dataloader.loader;

import com.arou.luna.dataloader.model.Product;
import com.arou.luna.dataloader.model.Ratting;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class RattingDataLoader {
    public static void main(String[] args) {
        String pathFile = "H:\\flink-project\\ratings.csv";
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Ratting> rattingDataSet = environment.readCsvFile(pathFile)
                .fieldDelimiter(",")
                .lineDelimiter("\n")
                .ignoreFirstLine()
                .includeFields(true, true, true, true)
                .pojoType(Ratting.class,
                        "userId",
                        "productId",
                        "score",
                        "timestamp");
//        rattingDataSet.mapPartition(new MapPartitionFunction<Ratting, Ratting>() {
//            @Override
//            public void mapPartition(Iterable<Ratting> iterable, Collector<Ratting> collector) throws Exception {
//                System.out.println(iterable.iterator().hasNext());
//                insertIntoMongo(iterable, collector);
//            }
//        });
//        try {
//            rattingDataSet.count();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        try {
            buildConnection(rattingDataSet.collect());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertIntoMongo(Iterable<Ratting> iterable, Collector<Ratting> collector) {
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        ServerAddress address = new ServerAddress("106.12.145.41", 27017);
        credentials.add(MongoCredential.createCredential("luna", "luna", "luna1748".toCharArray()));
        MongoClient mongoClient = new MongoClient(address, credentials);
        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("luna");
        System.out.println("Connect to database successfully");
        mongoDatabase.createCollection("Ratting");
        MongoCollection<Document> collection = mongoDatabase.getCollection("Ratting");
        List<Document> documents = new ArrayList<>();

        for (Ratting ratting : iterable) {
            Document document = new Document("userId", ratting.getUserId()).
                    append("productId", ratting.getProductId()).
                    append("score", ratting.getScore()).
                    append("timestamp", ratting.getTimestamp());
            documents.add(document);
            collector.collect(ratting);
        }
        collection.insertMany(documents);
        System.out.println("成功了插入了" + documents.size() + "行数据");
    }

    private static void buildConnection(List<Ratting> collect) {
        System.out.println("连接 mongoDB ");
        // 连接到 mongodb 服务
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        ServerAddress address = new ServerAddress("106.12.145.41", 27017);
        credentials.add(MongoCredential.createCredential("luna", "luna", "luna1748".toCharArray()));
        MongoClient mongoClient = new MongoClient(address, credentials);
        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("luna");
        System.out.println("Connect to database successfully");
        mongoDatabase.createCollection("Ratting");
        MongoCollection<Document> collection = mongoDatabase.getCollection("Ratting");
        List<Document> documents = new ArrayList<>();
        //遍历数据集合
        for (Ratting ratting : collect) {
            Document document = new Document("userId", ratting.getUserId()).
                    append("productId", ratting.getProductId()).
                    append("score", ratting.getScore()).
                    append("timestamp", ratting.getTimestamp());
            documents.add(document);
        }
        collection.insertMany(documents);
        System.out.println("成功了插入了" + documents.size() + "行数据");

    }

}
