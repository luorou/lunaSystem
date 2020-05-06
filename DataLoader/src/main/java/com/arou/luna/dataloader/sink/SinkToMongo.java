package com.arou.luna.dataloader.sink;
import com.arou.luna.dataloader.model.Product;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;

public class SinkToMongo extends RichSinkFunction<List<Product>> {
    static MongoClient mongoClient = null;
    static MongoDatabase mongoDatabase = null;

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
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Product> value, Context context) throws Exception {
        MongoCollection<Document> collection = mongoDatabase.getCollection("Product");
        List<Document> documents = new ArrayList<>();
        //遍历数据集合
        for (Product product : value) {
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


    private static void buildConnection() {
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
}
