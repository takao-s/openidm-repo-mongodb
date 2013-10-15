package org.forgerock.openidm.repo.mongodb.impl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.forgerock.json.fluent.JsonValue;
// JSON Resource
import org.forgerock.json.resource.JsonResource;
import org.forgerock.openidm.config.EnhancedConfig;
import org.forgerock.openidm.config.InvalidException;
import org.forgerock.openidm.config.JSONEnhancedConfig;
// Deprecated
import org.forgerock.openidm.objset.BadRequestException;
import org.forgerock.openidm.objset.ConflictException;
import org.forgerock.openidm.objset.ForbiddenException;
import org.forgerock.openidm.objset.NotFoundException;
import org.forgerock.openidm.objset.ObjectSetException;
import org.forgerock.openidm.objset.ObjectSetJsonResource;
import org.forgerock.openidm.objset.Patch;
import org.forgerock.openidm.objset.PreconditionFailedException;
import org.forgerock.openidm.repo.QueryConstants;
import org.forgerock.openidm.repo.RepoBootService;
import org.forgerock.openidm.repo.RepositoryService;
import org.forgerock.openidm.repo.mongodb.impl.query.PredefinedQueries;
import org.forgerock.openidm.repo.mongodb.impl.query.Queries;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

/**
 * Repository service implementation using MongoDB
 * @author takao-s
 */
@Component(name = MongoDBRepoService.PID, immediate=true, policy=ConfigurationPolicy.REQUIRE, enabled=true)
@Service (value = {RepositoryService.class, JsonResource.class}) // Omit the RepoBootService interface from the managed service
@Properties({
    @Property(name = "service.description", value = "Repository Service using MongoDB"),
    @Property(name = "service.vendor", value = "ForgeRock AS"),
    @Property(name = "openidm.router.prefix", value = "repo"),
    @Property(name = "db.type", value = "MongoDB")
})
public class MongoDBRepoService extends ObjectSetJsonResource implements RepositoryService, RepoBootService {
    final static Logger logger = LoggerFactory.getLogger(MongoDBRepoService.class);

    public static final String PID = "org.forgerock.openidm.repo.mongodb";
    
    // Keys in the JSON configuration
    public static final String CONFIG_REPLICASET = "replicaSet";
    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_DBNAME = "dbName";
    public static final String CONFIG_CONN_PER_HOST = "connectionPerHost";
    public static final String CONFIG_CONN_MULTIPLIER = "connectionMultiple";


    public static final String CONFIG_QUERIES = "queries";
    public static final String CONFIG_QUERY_FIELDS = "fields";
    public static final String CONFIG_QUERY_SORT = "sort";
    public static final String CONFIG_QUERY_AGGREGATE = "aggregate";

    public static final String CONFIG_USER = "user";
    public static final String CONFIG_PASSWORD = "password";
    
    public static final String CONFIG_DB_COLLECTIONS = "collections";
    public static final String CONFIG_INDEX = "index";    
    public static final String CONFIG_INDEX_UNIQUE = "unique";
    
    DB db;
    
    // Current configuration
    JsonValue existingConfig;
    
    PredefinedQueries predefinedQueries = new PredefinedQueries();
    Queries queries = new Queries();
    EnhancedConfig enhancedConfig = new JSONEnhancedConfig();
    
    /**
     * Gets an object from the repository by identifier. The returned object is not validated 
     * against the current schema and may need processing to conform to an updated schema.
     * <p>
     *
     * @param fullId the identifier of the object to retrieve from the object set.
     * @throws NotFoundException if the specified object could not be found. 
     * @throws ForbiddenException if access to the object is forbidden.
     * @throws BadRequestException if the passed identifier is invalid
     * @return the requested object.
     */
    @Override
    public Map<String, Object> read(String fullId) throws ObjectSetException {
        String localId = getLocalId(fullId);
        String type = getObjectType(fullId);
        
        if (fullId == null || localId == null) {
            throw new NotFoundException("The repository requires clients to supply an identifier "
                    + "for the object to create. Full identifier: " + fullId + " local identifier: " + localId);
        } else if (type == null) {
            throw new NotFoundException("The object identifier did not include "
                    + "sufficient information to determine the object type: " + fullId);
        }
        
        Map<String, Object> result = null;
        DBCollection collection = getCollection(type);
        DBObject doc = predefinedQueries.getByID(localId, collection);
        if (doc == null) {
            throw new NotFoundException("Object " + fullId + " not found in " + type);
        }
        result = doc.toMap();
        logger.trace("Completed get for id: {} result: {}", fullId, result);
        return result;
    }

    /**
     * Creates a new object in the object set.
     * <p>
     * This method sets the {@code _id} property to the assigned identifier for the object,
     * and the {@code _rev} property to the revised object version (For optimistic concurrency)
     *
     * @param fullId the client-generated identifier to use, or {@code null} if server-generated identifier is requested.
     * @param obj the contents of the object to create in the object set.
     * @throws NotFoundException if the specified id could not be resolved. 
     * @throws ForbiddenException if access to the object or object set is forbidden.
     * @throws PreconditionFailedException if an object with the same ID already exists.
     */
    @Override
    public void create(String fullId, Map<String, Object> obj) throws ObjectSetException {
        String localId = getLocalId(fullId);
        String type = getObjectType(fullId);
        
        if (fullId == null || localId == null) {
            throw new NotFoundException("The repository requires clients to supply an identifier for the object to create. Full identifier: " + fullId + " local identifier: " + localId);
        } else if (type == null) {
            throw new NotFoundException("The object identifier did not include sufficient information to determine the object type: " + fullId);
        }
        
        obj.put(DocumentUtil.TAG_ID, localId);
        obj.put(DocumentUtil.MONGODB_PRIMARY_KEY, localId);
        obj.put(DocumentUtil.TAG_REV, "0");
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start(obj);
        DBObject jo = builder.get();
        
        DBCollection collection = getCollection(type);
        collection.insert(jo);
        logger.debug("Completed create for id: {} revision: {}", fullId, jo.get(DocumentUtil.TAG_REV));
        logger.trace("Create payload for id: {} doc: {}", fullId, jo);
    }
    
    /**
     * Updates the specified object in the object set. 
     * <p>
     * This implementation requires MVCC and hence enforces that clients state what revision they expect 
     * to be updating
     * 
     * If successful, this method updates metadata properties within the passed object,
     * including: a new {@code _rev} value for the revised object's version
     *
     * @param fullId the identifier of the object to be put, or {@code null} to request a generated identifier.
     * @param rev the version of the object to update; or {@code null} if not provided.
     * @param obj the contents of the object to put in the object set.
     * @throws ConflictException if version is required but is {@code null}.
     * @throws ForbiddenException if access to the object is forbidden.
     * @throws NotFoundException if the specified object could not be found. 
     * @throws PreconditionFailedException if version did not match the existing object in the set.
     * @throws BadRequestException if the passed identifier is invalid
     */
    @Override
    public void update(String fullId, String rev, Map<String, Object> obj) throws ObjectSetException {
        
        String localId = getLocalId(fullId);
        String type = getObjectType(fullId);
        
        if (rev == null) {
            throw new ConflictException("Object passed into update does not have revision it expects set.");
        } else {
            DocumentUtil.parseVersion(rev);
            obj.put(DocumentUtil.TAG_REV, rev);
        }
        
        DBCollection collection = getCollection(type);
        DBObject existingDoc = predefinedQueries.getByID(localId, collection);
        if (existingDoc == null) {
            throw new NotFoundException("Update on object " + fullId + " could not find existing object.");
        }
        
        obj.remove(DocumentUtil.TAG_ID);
        obj.put(DocumentUtil.MONGODB_PRIMARY_KEY, localId);
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start(obj);
        DBObject jo = builder.get();
        WriteResult res = collection.update(new BasicDBObject(DocumentUtil.TAG_ID, localId), jo);
        logger.trace("Updated doc for id {} to save {}", fullId, jo);
    }

    /**
     * Deletes the specified object from the object set.
     *
     * @param fullId the identifier of the object to be deleted.
     * @param rev the version of the object to delete or {@code null} if not provided.
     * @throws NotFoundException if the specified object could not be found. 
     * @throws ForbiddenException if access to the object is forbidden.
     * @throws ConflictException if version is required but is {@code null}.
     * @throws PreconditionFailedException if version did not match the existing object in the set.
     */
    @Override
    public void delete(String fullId, String rev) throws ObjectSetException {
        String localId = getLocalId(fullId);
        String type = getObjectType(fullId);

        if (rev == null) {
            throw new ConflictException("Object passed into delete does not have revision it expects set.");
        }
        
        int ver = Integer.valueOf(DocumentUtil.parseVersion(rev)); // This throws ConflictException if parse fails
        
        DBCollection collection = getCollection(type);
        DBObject existingDoc = predefinedQueries.getByID(localId, collection);
        if (existingDoc == null) {
            throw new NotFoundException("Object does not exist for delete on: " + fullId);
        }
        
        WriteResult res = collection.remove(new BasicDBObject(DocumentUtil.TAG_ID, localId));
    }

    /**
     * Currently not supported by this implementation.
     * 
     * Applies a patch (partial change) to the specified object in the object set.
     *
     * @param id the identifier of the object to be patched.
     * @param rev the version of the object to patch or {@code null} if not provided.
     * @param patch the partial change to apply to the object.
     * @throws ConflictException if patch could not be applied object state or if version is required.
     * @throws ForbiddenException if access to the object is forbidden.
     * @throws NotFoundException if the specified object could not be found. 
     * @throws PreconditionFailedException if version did not match the existing object in the set.
     */
    @Override
    public void patch(String id, String rev, Patch patch) throws ObjectSetException {
        throw new UnsupportedOperationException();
    }

    /**
     * Performs the query on the specified object and returns the associated results.
     * <p>
     * Queries are parametric; a set of named parameters is provided as the query criteria.
     * The query result is a JSON object structure composed of basic Java types. 
     * 
     * The returned map is structured as follow: 
     * - The top level map contains meta-data about the query, plus an entry with the actual result records.
     * - The <code>QueryConstants</code> defines the map keys, including the result records (QUERY_RESULT)
     *
     * @param fullId identifies the object to query.
     * @param params the parameters of the query to perform.
     * @return the query results, which includes meta-data and the result records in JSON object structure format.
     * @throws NotFoundException if the specified object could not be found. 
     * @throws BadRequestException if the specified params contain invalid arguments, e.g. a query id that is not
     * configured, a query expression that is invalid, or missing query substitution tokens.
     * @throws ForbiddenException if access to the object or specified query is forbidden.
     */
    @Override
    public Map<String, Object> query(String fullId, Map<String, Object> params) throws ObjectSetException {
        String type = getObjectType(fullId); 
        logger.trace("Full id: {} Extracted type: {}", fullId, type);
        
        Map<String, Object> result = new HashMap<String, Object>();
        DBCollection collection = getCollection(type);
        
        List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
        result.put(QueryConstants.QUERY_RESULT, docs);
        long start = System.currentTimeMillis();
        List<DBObject> queryResult = queries.query(params, collection); 
        long end = System.currentTimeMillis();
        if (queryResult != null) {
            long convStart = System.currentTimeMillis();
            for (DBObject entry : queryResult) {
                Map<String, Object> convertedEntry = entry.toMap();
                docs.add(convertedEntry);
            }
            long convEnd = System.currentTimeMillis();
            result.put(QueryConstants.STATISTICS_CONVERSION_TIME, Long.valueOf(convEnd-convStart));
        }
        result.put(QueryConstants.STATISTICS_QUERY_TIME, Long.valueOf(end-start));
        
        if (logger.isDebugEnabled()) {
            logger.debug("Query result contains {} records, took {} ms and took {} ms to convert result.",
                    new Object[] {((List) result.get(QueryConstants.QUERY_RESULT)).size(),
                    result.get(QueryConstants.STATISTICS_QUERY_TIME),
                    result.get(QueryConstants.STATISTICS_CONVERSION_TIME)});
        }
        return result;
    }

    @Override
    public Map<String, Object> action(String id, Map<String, Object> params) throws ObjectSetException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param name
     * @return A MongoDB collection.
     */
    DBCollection getCollection(String name) {
        return db.getCollection(typeToCollectionName(name));
    }
    
    private static String getLocalId(String id) {
        String localId = null;
        int lastSlashPos = id.lastIndexOf("/");
        if (lastSlashPos > -1) {
            localId = id.substring(id.lastIndexOf("/") + 1);
        }
        logger.trace("Full id: {} Extracted local id: {}", id, localId);
        return localId;
    }
    
    private static String getObjectType(String id) {
        String type = null;
        
        int startPos = 0;
        if (id.startsWith("/")) {
            startPos = 1;
        }
        int lastSlashPos = id.lastIndexOf("/");

        if (lastSlashPos > startPos) {
            type = id.substring(startPos, lastSlashPos);
        } else {
            type = id.substring(startPos);
        }
        logger.trace("Full id: {} Extracted type: {}", id, type);
        return type;
    }
    
    public static String typeToCollectionName(String type) {
        return type.replace("/", "_");
    }
    
    /**
     * Populate and return a repository service that knows how to query and manipulate configuration.
     *
     * @param repoConfig the bootstrap configuration
     * @return the boot repository service. This instance is not managed by SCR and needs to be manually registered.
     */
    static MongoDBRepoService getRepoBootService(JsonValue repoConfig) {
        MongoDBRepoService bootRepo = new MongoDBRepoService();
        bootRepo.init(repoConfig);
        return bootRepo;
    }
    
    
    /**
     * Initialize the instnace with the given configuration.
     * 
     * This can configure managed (DS/SCR) instances, as well as explicitly instantiated
     * (bootstrap) instances.
     * 
     * @param config the configuration
     */
    void init (JsonValue config) {        
        db = DBHelper.getDB(config, true);
        
        queries.setQueriesConfig(config.get(CONFIG_QUERIES).toString());
        queries.setFieldsConfig(config.get(CONFIG_QUERY_FIELDS).toString());
        queries.setSortConfig(config.get(CONFIG_QUERY_SORT).toString());
        queries.setAggregationConfig(config.get(CONFIG_QUERY_AGGREGATE).toString());
    }
    
    
    /**
     * Activates the MongoDB Repository Service
     * 
     * @param compContext   The component context
     */
    @Activate
    void activate(ComponentContext compContext) throws Exception { 
        logger.debug("Activating Service with configurati"
                + "on {}", compContext.getProperties());
        try {
            existingConfig = enhancedConfig.getConfigurationAsJson(compContext);
        } catch (RuntimeException ex) {
            logger.warn("Configuration invalid and could not be parsed, can not start MongoDB repository: " 
                    + ex.getMessage(), ex);
            throw ex;
        }
        
        init(existingConfig);
        
        logger.info("Repository started.");
    }

    /**
     * Handle an existing activated service getting changed; 
     * e.g. configuration changes or dependency changes
     * 
     * @param compContext THe OSGI component context
     * @throws Exception if handling the modified event failed
     */
    @Modified
    void modified(ComponentContext compContext) throws Exception {
        logger.debug("Handle repository service modified notification");
        JsonValue newConfig = null;
        try {
            newConfig = enhancedConfig.getConfigurationAsJson(compContext);
        } catch (RuntimeException ex) {
            logger.warn("Configuration invalid and could not be parsed, can not start MongoDB repository", ex); 
            throw ex;
        }
        if (existingConfig != null) {
            logger.info("(Re-)initialize repository with latest configuration.");
            init(newConfig);
        } else {
            // If the connection settings changed do a more complete re-initialization
            logger.info("Re-initialize repository with latest configuration - including connection setting changes.");
            deactivate(compContext);
            activate(compContext);
        }
        
        existingConfig = newConfig;
        logger.debug("Repository service modified");
    }
    
    @Deactivate
    void deactivate(ComponentContext compContext) {
        DBHelper.close();
        logger.debug("Deactivating Service {}", compContext);
    }
}
