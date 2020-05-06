package com.arou.luna.dataloader.loader;

import com.arou.luna.dataloader.format.ProductOutputFormat;
import com.arou.luna.dataloader.model.Product;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ProductDataLoader {
    public static void main(String[] args) {
        String pathFile = "H:\\flink-project\\products.csv";
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Product> productDataSet = environment.readTextFile(pathFile).map(new MapFunction<String, String[]>() {
            @Override
            public String[] map(String s) throws Exception {
                return s.split("\\^");
            }
        }).flatMap(new FlatMapFunction<String[], Product>() {
            @Override
            public void flatMap(String[] strings, Collector<Product> collector) throws Exception {
                Product product = new Product(Long.parseLong(strings[0]), strings[1], strings[4], strings[5], strings[6]);
                collector.collect(product);
            }
        });
        try {
            buildConnection(productDataSet.collect());
        } catch (Exception e) {
            e.printStackTrace();
        }
//        productDataSet.output(new ProductOutputFormat());
//        try {
//            environment.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    private static void buildConnection(List<Product> collect) {
        System.out.println("连接 mongoDB ");
        // 连接到 mongodb 服务
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        ServerAddress address = new ServerAddress("106.12.145.41", 27017);
        credentials.add(MongoCredential.createCredential("luna", "luna", "luna1748".toCharArray()));
        MongoClient mongoClient = new MongoClient(address, credentials);
        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("luna");
        System.out.println("Connect to database successfully");

        MongoCollection<Document> collection = mongoDatabase.getCollection("Product");
        List<Document> documents = new ArrayList<>();
        //遍历数据集合
        for (Product product : collect) {
            Document document = new Document("productId", product.getProductId()).
                    append("name", product.getName()).
                    append("imageUrl", product.getImageUrl()).
                    append("categories", product.getCategories()).
                    append("tags", product.getTags());
            documents.add(document);
        }
        collection.insertMany(documents);
        System.out.println("成功了插入了" + documents.size() + "行数据");

    }

}
