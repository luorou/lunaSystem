package com.arou.luna.statistics.source;

import com.arou.luna.statistics.model.Product;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ProductMongoSourceFunction extends RichSourceFunction<Product> {
    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase = null;
    MongoCollection<Document> collection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        ServerAddress address = new ServerAddress("106.12.145.41", 27017);
        credentials.add(MongoCredential.createCredential("luna", "luna", "luna1748".toCharArray()));
        mongoClient = new MongoClient(address, credentials);
        mongoDatabase = mongoClient.getDatabase("luna");
        collection = mongoDatabase.getCollection("Product");
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        for (Document document : collection.find()) {
            Product product = new Product(
                    document.getLong("productId"),
                    document.getString("name"),
                    document.getString("imageUrl"),
                    document.getString("categories"),
                    document.getString("tags"));
            sourceContext.collect(product);
        }
    }

    @Override
    public void cancel() {
        mongoClient.close();
    }
}
