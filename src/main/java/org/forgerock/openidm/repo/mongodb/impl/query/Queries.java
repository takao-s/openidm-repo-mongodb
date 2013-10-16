package org.forgerock.openidm.repo.mongodb.impl.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.objset.BadRequestException;
import org.forgerock.openidm.repo.QueryConstants;
import org.forgerock.openidm.smartevent.EventEntry;
import org.forgerock.openidm.smartevent.Name;
import org.forgerock.openidm.smartevent.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Configured and add-hoc query support on MongoDB
 * 
 * Queries can contain tokens of the format ${token-name}
 * 
 * @author takao-s
 *
 */
public class Queries {

    final static Logger logger = LoggerFactory.getLogger(Queries.class);

    // Monitoring event name prefix
    static final String EVENT_RAW_QUERY_PREFIX = "openidm/internal/repo/mongodb/raw/query/";
    
    TokenHandler tokenHandler = new TokenHandler();
    
    // Pre-configured queries, key is query id
    Map<String, QueryInfo> configuredQueries = new HashMap<String, QueryInfo>();
    /**
     * Execute a query, either a pre-configured query by using the query ID, or a query expression passed as 
     * part of the params.
     * 
     * The keys for the input parameters as well as the return map entries are in QueryConstants.
     * 
     * @param params the parameters which include the query id, or the query expression, as well as the 
     *        token key/value pairs to replace in the query
     * @param collection a handle to a MongoDB collection instance for exclusive use by the query method whilst it is executing.
     * @return The query result, which includes meta-data about the query, and the result set itself.
     * @throws BadRequestException if the passed request parameters are invalid, e.g. missing query id or query expression or tokens.
     */
    public List<DBObject> query(Map<String, Object> params, DBCollection collection) 
            throws BadRequestException {
        
        List<DBObject> result = null;
        QueryInfo foundQuery = null;
        params.put(QueryConstants.RESOURCE_NAME, collection.getName()); 
        
        String queryExpression = (String) params.get(QueryConstants.QUERY_EXPRESSION);
        String queryId = null;
        if (queryExpression != null) {
            String queryString = (String) params.get(QueryConstants.QUERY_EXPRESSION);
            foundQuery = new QueryInfo(false);
            foundQuery.setQuery(queryString);
        } else {
            queryId = (String) params.get(QueryConstants.QUERY_ID);
            if (queryId == null) {
                throw new BadRequestException("Either " + QueryConstants.QUERY_ID + " or " + QueryConstants.QUERY_EXPRESSION
                        + " to identify/define a query must be passed in the parameters. " + params);
            }
            foundQuery = configuredQueries.get(queryId);
            if (foundQuery == null) {
                throw new BadRequestException("The passed query identifier " + queryId 
                        + " does not match any configured queries on the MongoDB repository service.");
            }
        }
        
        if (foundQuery != null) {
            DBObject query = null;
            
            logger.debug("Evaluate query {}", query);
            Name eventName = getEventName(queryExpression, queryId);
            EventEntry measure = Publisher.start(eventName, foundQuery, null);
            try {
                result = executeQuery(foundQuery, params, collection);
                measure.setResult(result);
            } catch (BadRequestException e) {
                throw new BadRequestException("Failed to resolve and parse the query " 
                      + queryId + " with params: " + params);
            } catch (RuntimeException ex) {
                logger.warn("Unexpected failure during DB query: {}", ex.getMessage());
                throw ex;
            } finally {
                measure.end();
            }
        }
        return result;
    }
    
    public void setQueriesConfig(String queriesConfig) {
        DBObject o = (DBObject)JSON.parse(queriesConfig);
        for (String key : o.keySet()) {
            QueryInfo qi = new QueryInfo(false);
            qi.setQuery(o.get(key).toString());
            this.configuredQueries.put(key, qi);
        }
    }
    
    public void setFieldsConfig(String fieldsConfig) {
        DBObject o = (DBObject)JSON.parse(fieldsConfig);
        for (String key : o.keySet()) {
            QueryInfo qi = new QueryInfo(false);
            if (configuredQueries.containsKey(key)) {
                qi = configuredQueries.get(key);
            }
            qi.setFileds(o.get(key).toString());
            this.configuredQueries.put(key, qi);
        }
    }
    
    public void setSortConfig(String sortConfig) {
        DBObject o = (DBObject)JSON.parse(sortConfig);
        for (String key : o.keySet()) {
            QueryInfo qi = new QueryInfo(false);
            if (configuredQueries.containsKey(key)) {
                qi = configuredQueries.get(key);
            }
            qi.setSort(o.get(key).toString());
            this.configuredQueries.put(key, qi);
        }
    }
    
    public void setAggregationConfig(String aggregationCofnig) {
        DBObject o = (DBObject)JSON.parse(aggregationCofnig);
        for (String key : o.keySet()) {
            QueryInfo qi = new QueryInfo(true);
            
            List<Object> list = (List<Object>)o.get(key);
            List<String> params = new ArrayList<String>();
            for (Object obj : list) {
                params.add(obj.toString());
            }
            
            qi.setAggregationParams(params);
            this.configuredQueries.put(key, qi);
        }
    }
    
    /**
     * Resolve the query string which can contain ${token} tokens to a fully resolved query
     * Doing the resolution ourselves means it can not be a prepared statement with tokens
     * that gets re-used, but it allows us to replace more parts of the query than just
     * the where clause.
     *  
     * @param queryString The query with tokens
     * @param params THe parameters to replace the tokens with
     * @return DBObject with any found tokens replaced
     * @throws BadRequestException if the queryString contains token missing from params
     */
    protected DBObject resolveQuery(String queryString, Map<String,Object> params)
            throws BadRequestException {
        String resolvedQueryString = tokenHandler.replaceTokensWithValues(queryString, params);
        DBObject query = (DBObject) JSON.parse(resolvedQueryString);
        return query;
    }
    
    protected List<DBObject> executeQuery(QueryInfo queryInfo, Map<String,Object> params,
            DBCollection collection) throws BadRequestException {
        List<DBObject> result = null;
        if (queryInfo.isGroupQuery()) {
            String resultKey ="";
            List<String> list = queryInfo.getAggregationParams();
            List<DBObject> dboList = new ArrayList<DBObject>();
            DBObject firstParam = new BasicDBObject();
            boolean first = true;
            for (String s : list) {
                DBObject query = resolveQuery(s, params);
                if (first) {
                    firstParam = query;
                    first = !first;
                } else {
                    dboList.add(query);
                }
            }
            
            AggregationOutput output = collection.aggregate(firstParam,
                    (DBObject[])dboList.toArray(new BasicDBObject[0]));
            if (output.results().iterator().hasNext()) {
                result = new ArrayList<DBObject>();
            }
            for (DBObject obj : output.results()) {
                result.add(obj);
            }
        } else {
            String q = (queryInfo.getQuery() == null) ? "{}" : queryInfo.getQuery();
            DBObject query = resolveQuery(q, params);
            String f = (queryInfo.getFileds() == null) ? "{}" : queryInfo.getFileds();
            DBObject fields = resolveQuery(f, params);
            String s = (queryInfo.getSort() == null) ? "{}" : queryInfo.getSort();
            DBObject sort = resolveQuery(s, params);
            
            DBCursor cur = null;
            try {
                cur = collection.find(query, fields).sort(sort);
                result = cur.toArray();
            } catch (Exception ex) {
                throw new BadRequestException(ex.getMessage());
            } finally {
                cur.close();
            }
        }
        return result;
    }

    /**
     * @return the smartevent Name for a given query
     */
    Name getEventName(String queryExpression, String queryId) {
        if (queryId == null) {
            return Name.get(EVENT_RAW_QUERY_PREFIX + "_query_expression");
        } else {
            return Name.get(EVENT_RAW_QUERY_PREFIX + queryId);
        }
    }
    
    public QueryInfo getQueryInfo(String name) {
        return configuredQueries.get(name);
    }
}
