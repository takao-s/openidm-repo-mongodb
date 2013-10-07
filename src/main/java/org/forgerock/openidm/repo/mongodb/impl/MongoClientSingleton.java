package org.forgerock.openidm.repo.mongodb.impl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.config.InvalidException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

public enum MongoClientSingleton {
    INSTANCE;
    final static Logger logger = LoggerFactory.getLogger(MongoClientSingleton.class);
    private MongoClient client;

    public MongoClient getClient(JsonValue config) {
        if (client == null) {
            JsonValue connPerHost = config.get(MongoDBRepoService.CONFIG_CONN_PER_HOST);
            JsonValue connMultiple = config.get(MongoDBRepoService.CONFIG_CONN_MULTIPLIER);
            int connectionsPerHost = (connPerHost.isNull() ? 100 : connPerHost.asInteger());
            int connectonMultiple = (connMultiple.isNull() ? 5 : connMultiple.asInteger());;
            
            MongoClientOptions options 
                = new MongoClientOptions.Builder()
                    .connectionsPerHost(connectionsPerHost)
                    .threadsAllowedToBlockForConnectionMultiplier(connectonMultiple)
                    .build();

            List<ServerAddress> replicaSet = getReplicaSet(config);
            client = new MongoClient(replicaSet, options);
        }
        return client;
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
            Map<String, Object> map = ite.next().asMap();
            try {
                list.add(new ServerAddress(
                    map.get(MongoDBRepoService.CONFIG_HOST).toString(),
                    Integer.parseInt(map.get(MongoDBRepoService.CONFIG_PORT).toString())));
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
