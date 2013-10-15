package org.forgerock.openidm.repo.mongodb.impl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.config.InvalidException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

/**
 * A Helper to interact with the MongoDB
 * @author takao-s
 */
public class DBHelper {
    final static Logger logger = LoggerFactory.getLogger(DBHelper.class);
    
    /**
     * Get the MongoDB for given config.
     * 
     * @param config the full configuration for the DB
     * @param setupDB true if it create default users.
     * @return DB
     * @throws InvalidException
     */
    public synchronized static DB getDB(JsonValue config, boolean setupDB) throws InvalidException {
        String dbName = config.get(MongoDBRepoService.CONFIG_DBNAME).asString();
        String user = config.get(MongoDBRepoService.CONFIG_USER)
                .defaultTo("openidm").asString();
        char[] pass = config.get(MongoDBRepoService.CONFIG_PASSWORD)
                .defaultTo("openidm").asString().toCharArray();
        
        MongoClient client = MongoClientSingleton.INSTANCE.getClient(config);
        DB db = client.getDB(dbName);
        
        if (setupDB) {
            if (!client.getDatabaseNames().contains(dbName)) {
                db.addUser(user, pass);
            }
            ensureIndexes(db, config);
            DBCollection collection = db.getCollection("internal_user");
            populateDefaultUsers(collection, config);
        }
        if (!db.authenticate(user, pass)){
            logger.warn("Database Connection refused.");
            MongoClientSingleton.INSTANCE.close();
            throw new InvalidException();
        }
        return db;
    }
    
    public static void close() {
        MongoClientSingleton.INSTANCE.close();
    }
    
    // Populates the default user, the pwd needs to be changed by the installer
    private static void populateDefaultUsers(DBCollection collection, JsonValue completeConfig) 
            throws InvalidException {
        
        String defaultAdminUser = "openidm-admin";
        // Default password needs to be replaced after installation
        String defaultAdminPwd = "{\"$crypto\":{\"value\":{\"iv\":\"fIevcJYS4TMxClqcK7covg==\",\"data\":"
                + "\"Tu9o/S+j+rhOIgdp9uYc5Q==\",\"cipher\":\"AES/CBC/PKCS5Padding\",\"key\":\"openidm-sym-default\"},"
                + "\"type\":\"x-simple-encryption\"}}";
        String defaultAdminRoles = "openidm-admin,openidm-authorized";
        populateDefaultUser(collection, completeConfig, defaultAdminUser, 
                defaultAdminPwd, defaultAdminRoles);
        logger.trace("Created default user {}. Please change the assigned default password.", 
                defaultAdminUser);
        
        String anonymousUser = "anonymous";
        String anonymousPwd = "anonymous";
        String anonymousRoles = "openidm-reg";
        populateDefaultUser(collection, completeConfig, anonymousUser, anonymousPwd, anonymousRoles);
        logger.trace("Created default user {} for registration purposes.", anonymousUser);
    }
    
    private static void ensureIndexes(DB db, JsonValue config) {
        Map<String, Object> map = config.get(MongoDBRepoService.CONFIG_DB_COLLECTIONS).asMap();
        for (Entry<String, Object> entry : map.entrySet() ){
            DBCollection collection = db.getCollection(entry.getKey());
            
            Map<String, Object> collectionIndexes = (Map<String, Object>) entry.getValue();
            List<Object> listIdx = (List<Object>)collectionIndexes.get(MongoDBRepoService.CONFIG_INDEX);

            if (listIdx.get(0) instanceof Map) {
                ensureIndex(collection, listIdx);
            } else {
                for (Object indexObj : listIdx) {
                    List<Object> list = (List<Object>)indexObj;
                    ensureIndex(collection, list);
                }
            }
        }
    }
    
    private static void ensureIndex (DBCollection collection, List<Object> list) {
        DBObject keys = null;
        boolean unique = false;
        for (Object o : list) {
            Map m = (Map)o;
            if (m.containsKey(MongoDBRepoService.CONFIG_INDEX_UNIQUE)) {
                unique = true;
            } else {
                keys = new BasicDBObject(m);
            }
        }
        collection.ensureIndex(keys, null, unique);
    }
    
    private static void populateDefaultUser(DBCollection collection, 
            JsonValue completeConfig, String user, String pwd, String roles) {
        
        DBObject query = (DBObject)JSON.parse("{\"_openidm_id\": \""+user+"\"}");
        if (collection.count(query) > 0) {
            return;
        }
        Map<String, Object> defAdmin = new LinkedHashMap<String, Object>();
        defAdmin.put("_openidm_id", user);
        defAdmin.put("userName", user);
        defAdmin.put("password", pwd);
        String[] role = roles.split(",");
        defAdmin.put("roles", role);
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start(defAdmin);
        
        DBObject o = builder.get();
        collection.insert(o);
    }
}
