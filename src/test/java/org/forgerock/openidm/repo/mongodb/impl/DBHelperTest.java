/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright © 2011 ForgeRock AS.
 * Portions Copyrighted 2013 Takao Sekiguchi.
 * All rights reserved.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */
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

/**
 * DBHelper test
 * 
 * @author takao-s
 */
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
        
        query = new BasicDBObject();
        query.put("_openidm_id", "anonymous");
        DBObject r2 = collection.findOne(query);
        Assert.assertNotNull(r2);
        
        query = new BasicDBObject();
        query.put("_id", "openidm-admin");
        DBObject r3 = collection.findOne(query);
        Assert.assertNotNull(r3);
        
        query = new BasicDBObject();
        query.put("_id", "anonymous");
        DBObject r4 = collection.findOne(query);
        Assert.assertNotNull(r4);
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
