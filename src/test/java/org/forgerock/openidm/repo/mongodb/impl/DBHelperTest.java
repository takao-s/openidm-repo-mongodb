package org.forgerock.openidm.repo.mongodb.impl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.repo.mongodb.util.JsonReader;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class DBHelperTest {
    
    JsonValue config = new JsonValue(new LinkedHashMap<String, Object>());
    String host = "localhost";
    String port = "27017";
    String dbName = "openidm_test";
    String user = "openidm";
    String pass = "openidm";
    
    @BeforeTest
    public void setUp() {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> parsedConfig;
        String jsonConfig = new JsonReader().getJsonConfig();
        try {
            parsedConfig = mapper.readValue(jsonConfig, Map.class);
            config = new JsonValue(parsedConfig);
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ServerAddress addr = null;
        try {
            addr = new ServerAddress(host, Integer.valueOf(port));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @AfterMethod
    public void afterMethod() {
        ServerAddress addr = null;
        try {
            addr = new ServerAddress(host, Integer.valueOf(port));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        MongoClient client = new MongoClient(addr);
        DB db = client.getDB(dbName);
        db.dropDatabase();
    }

    @AfterTest
    public void tearDown() {
        ServerAddress addr = null;
        try {
            addr = new ServerAddress(host, Integer.valueOf(port));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        MongoClient client = new MongoClient(addr);
        DB db = client.getDB(dbName);
        db.dropDatabase();
    }
    @Test
    public void testGetDB_with_setup() {
        DB db = DBHelper.getDB(config, true);
        DBCollection collection = db.getCollection("internal_user");
        
        BasicDBObject query = new BasicDBObject();
        query.put("_openidm_id", "openidm-admin");
        DBObject r1 = collection.findOne(query);
        Assert.assertNotNull(r1);
        
        query.put("_openidm_id", "anonymous");
        DBObject r2 = collection.findOne(query);
        Assert.assertNotNull(r2);
        return;
    }
    
    @Test
    public void testGetDB_without_setup() {
        DB db = DBHelper.getDB(config, false);
        DBCollection collection = db.getCollection("internal_user");
        
        BasicDBObject query = new BasicDBObject();
        query.put("_openidm_id", "openidm-admin");
        DBObject r1 = collection.findOne(query);
        Assert.assertNull(r1);
        
        query.put("_openidm_id", "anonymous");
        DBObject r2 = collection.findOne(query);
        Assert.assertNull(r2);
        return;
    }
}
