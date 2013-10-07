package org.forgerock.openidm.repo.mongodb.impl.query;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.objset.BadRequestException;
import org.forgerock.openidm.repo.QueryConstants;
import org.forgerock.openidm.repo.mongodb.impl.DBHelper;
import org.forgerock.openidm.repo.mongodb.impl.MongoDBRepoService;
import org.forgerock.openidm.repo.mongodb.util.JsonReader;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Queries test
 * 
 * @author takao-s
 */
public class QueriesTest {

    private static ObjectMapper mapper = new ObjectMapper();
    private static String jsonConfig = new JsonReader().getJsonConfig();
    private static String jsonUsers = new JsonReader().getJsonUsers();
    private static JsonValue config = null;
    private static String collectionName = "managed_user";
    
    @BeforeClass
    public static void setUpClassl() {
        Map<String, Object> parsedConfig = null;
        try {
            parsedConfig = mapper.readValue(jsonConfig, Map.class);
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        config = new JsonValue(parsedConfig);
        DBCollection collection = DBHelper.getDB(config, true).getCollection(collectionName);
        
        List<DBObject> l = (List<DBObject>)JSON.parse(jsonUsers);
        for (DBObject user : l) {
            collection.insert(user);
        }
    }
    @AfterClass
    public static void tearDownClass() {
        DB db = DBHelper.getDB(config, false);
        db.dropDatabase();
    }

    @Test
    public void testSetQueriesConfig() {
        try {
            Queries queries = getQueries();
        } catch (JsonParseException e) {
            fail(e.getMessage());
        } catch (JsonMappingException e) {
            fail(e.getMessage());
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetFieldsConfig() {
        try {
            Queries queries = getQueriesWithFields();
        } catch (JsonParseException e) {
            fail(e.getMessage());
        } catch (JsonMappingException e) {
            fail(e.getMessage());
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetSortConfig() {
        try {
            Queries queries = getQueriesWithSort();
        } catch (JsonParseException e) {
            fail(e.getMessage());
        } catch (JsonMappingException e) {
            fail(e.getMessage());
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetAggregationConfig() throws JsonParseException, JsonMappingException, IOException {
        try {
            Queries queries = getQueriesOfAggregate();
        } catch (JsonParseException e) {
            fail(e.getMessage());
        } catch (JsonMappingException e) {
            fail(e.getMessage());
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void testResolveQuery() {
        Queries queries = new Queries();
        String qStr = "{ \"${field}\" : \"${value}\", \"$max\" : 345 }";
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("field", "userName");
        map.put("value", "Smith");
        map.put("max", "123");
        
        DBObject obj = null;
        try {
            obj = queries.resolveQuery(qStr, map);
        } catch (BadRequestException e) {
            fail(e.getMessage());
        }
        Assert.assertEquals(obj.containsField("userName"), true);
        Assert.assertEquals(obj.get("userName").toString(), "Smith");
        Assert.assertEquals(obj.get("$max").toString(), "345");
        Assert.assertNull(obj.get("max"));
    }
    
    @Test
    public void testExecuteQuery() {
        Queries queries = null;
        try {
            queries = getQueriesWithSort();
        } catch (Exception e) {
        }
        
        String queryName = "for-userName";
        QueryInfo queryInfo = queries.getQueryInfo(queryName);
        Map<String, Object> params = new LinkedHashMap<String, Object>();
        params.put("uid", "user010");
        DBCollection collection = DBHelper.getDB(config, true).getCollection(collectionName);
        
        try {
            List<DBObject> list = queries.executeQuery(queryInfo, params, collection);
            Assert.assertEquals(list.size(), 1);
            Assert.assertEquals(list.get(0).get("_openidm_id").toString(), "user010");
            Assert.assertEquals(list.get(0).get("userName").toString(), "user010");
            Assert.assertEquals(list.get(0).get("age").toString(), "20");
            Assert.assertEquals(list.get(0).get("gender").toString(), "female");
            Assert.assertEquals(list.get(0).get("mail").toString(), "user010@mail.test");
            return;
        } catch (BadRequestException e) {
            fail(e.getMessage());
        }
        fail("Not yet implemented");
    }
    
    @Test
    public void testQuery() {
        Queries queries = null;
        try {
            queries = getQueriesWithSort();
        } catch (Exception e) {
        }
        Map<String, Object> params = new LinkedHashMap<String, Object>();
        params.put(QueryConstants.QUERY_ID, "get-by-field-value");
        params.put("field", "gender");
        params.put("value", "male");
        DBCollection collection = DBHelper.getDB(config, true).getCollection(collectionName);
        
        try {
            List<DBObject> list = queries.query(params, collection);
            Assert.assertEquals(list.size(), 10);
            Assert.assertEquals(list.get(0).get("_openidm_id").toString(), "user001");
            Assert.assertEquals(list.get(0).get("userName").toString(), "user001");
            Assert.assertEquals(list.get(0).get("age").toString(), "11");
            Assert.assertEquals(list.get(0).get("gender").toString(), "male");
            Assert.assertEquals(list.get(0).get("mail").toString(), "user001@mail.test");
            return;
        } catch (BadRequestException e) {
            fail(e.getMessage());
        }
        
        fail("Not yet implemented");
    }
    
    
    
    /*** PRIVATE METHODS ***/
    
    private Queries getQueries() throws JsonParseException, JsonMappingException, IOException {
        Queries queries = new Queries();
        queries.setQueriesConfig(config.get(MongoDBRepoService.CONFIG_QUERIES).toString());
        return queries;
    }
    
    private Queries getQueriesWithFields() throws JsonParseException, JsonMappingException, IOException {
        Queries queries = new Queries();
        queries.setQueriesConfig(config.get(MongoDBRepoService.CONFIG_QUERIES).toString());
        
        queries.setFieldsConfig(config.get(MongoDBRepoService.CONFIG_QUERY_FIELDS).toString());
        return queries;
    }
    
    private Queries getQueriesWithSort() throws JsonParseException, JsonMappingException, IOException {
        Queries queries = new Queries();
        queries.setQueriesConfig(config.get(MongoDBRepoService.CONFIG_QUERIES).toString());
        
        queries.setFieldsConfig(config.get(MongoDBRepoService.CONFIG_QUERY_FIELDS).toString());
        
        queries.setSortConfig(config.get(MongoDBRepoService.CONFIG_QUERY_SORT).toString());

        return queries;
    }

    private Queries getQueriesOfAggregate() throws JsonParseException, JsonMappingException, IOException {
        Queries queries = new Queries();
        queries.setAggregationConfig(config.get(MongoDBRepoService.CONFIG_QUERY_AGGREGATE).toString());
        return queries;
    }
}
