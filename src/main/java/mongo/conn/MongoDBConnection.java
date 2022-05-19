package mongo.conn;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;

@Getter
public class MongoDBConnection {

    private MongoDatabase mongoDB;
    private MongoClient mongoClient;

    public MongoDBConnection() {

        String hostName = "192.168.137.134";
        int port = 27017;
        String userName = "MyUser";
        String password = "1234";
        String db = "MyDB";

        mongoClient = new MongoClient(hostName, port);

        MongoCredential.createCredential(userName, db, password.toCharArray());

        mongoDB = mongoClient.getDatabase(db);

    }


}
