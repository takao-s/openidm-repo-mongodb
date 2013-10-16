package org.forgerock.openidm.repo.mongodb.impl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.config.InvalidException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public enum MongoClientSingleton {
    INSTANCE;
    final static Logger logger = LoggerFactory.getLogger(MongoClientSingleton.class);
    private MongoClient client;
    private DB db;

    public MongoClientSingleton init(JsonValue config) {
        if (client == null) {
            JsonValue connPerHost = config.get(MongoDBRepoService.CONFIG_CONN_PER_HOST);
            JsonValue connMultiple = config.get(MongoDBRepoService.CONFIG_CONN_MULTIPLIER);
            int connectionsPerHost = (connPerHost.isNull() ? 100 : connPerHost.asInteger());
            int connectonMultiple = (connMultiple.isNull() ? 5 : connMultiple.asInteger());
            
            int w = 1;
            int wtimeout = 0;
            boolean j = false;
            JsonValue wc_conf = config.get(MongoDBRepoService.CONFIG_WRITE_CONCERN);
            if (wc_conf != null) {
                JsonValue jv_w = wc_conf.get("w");
                JsonValue jv_wtimeout = wc_conf.get("wtimeout");
                JsonValue jv_j = wc_conf.get("j");
                w = (jv_w.isNull() ? 1 : jv_w.asInteger());
                wtimeout = (jv_wtimeout.isNull() ? 0 : jv_wtimeout.asInteger());
                j = (jv_j.isNull() ? false : jv_j.asBoolean());
            }
            WriteConcern wc = new WriteConcern(w, wtimeout, false, j);
            
            MongoClientOptions options 
                = new MongoClientOptions.Builder()
                    .connectionsPerHost(connectionsPerHost)
                    .threadsAllowedToBlockForConnectionMultiplier(connectonMultiple)
                    .writeConcern(wc)
                    .build();
            List<ServerAddress> replicaSet = getReplicaSet(config);
            client = new MongoClient(replicaSet, options);
            db = getDB(config);
            logger.info("Create new MongoClient");
        }
        return this;
    }
    
    public MongoClient getClient() {
        return client;
    }
    
    private DB getDB(JsonValue config) {
        String dbName = config.get(MongoDBRepoService.CONFIG_DBNAME).asString();
        return client.getDB(dbName);
    }
    
    public DB getDB() {
        return db;
    }
    
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
    private static List<ServerAddress> getReplicaSet(JsonValue config) {
        JsonValue replicas = config.get(MongoDBRepoService.CONFIG_REPLICASET);
        List<ServerAddress> list = new ArrayList<ServerAddress>();

        for (Iterator<JsonValue> ite = replicas.iterator();ite.hasNext();) {
            JsonValue serverConf = ite.next();
            try {
                list.add(new ServerAddress(
                        serverConf.get(MongoDBRepoService.CONFIG_HOST).asString(),
                        serverConf.get(MongoDBRepoService.CONFIG_PORT).asInteger()));
            } catch (UnknownHostException ex) {
                logger.warn("Can't connect to Server", ex);
                throw new InvalidException();
            }
        }
        if (list.isEmpty()) {
            logger.warn("No Replica Set configured.");
            throw new InvalidException();
        }
        return list;
    }

}
