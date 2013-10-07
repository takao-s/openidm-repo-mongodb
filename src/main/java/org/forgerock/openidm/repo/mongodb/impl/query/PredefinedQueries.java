package org.forgerock.openidm.repo.mongodb.impl.query;

import org.forgerock.openidm.objset.BadRequestException;
import org.forgerock.openidm.repo.mongodb.impl.DocumentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Queries pre-defined by the system
 * 
 * @author takao-s
 */
public class PredefinedQueries {

    final static Logger logger = LoggerFactory.getLogger(PredefinedQueries.class);
    
    /**
     * Query by primary key, the OpenIDM identifier.
     * 
     * @param id the OpenIDM identifier for an object
     * @param database a handle to the MongoDB database object. No other thread must operate on this concurrently.
     * @return The DBObject if found, null if not found.
     * @throws BadRequestException if the passed identifier or type are invalid
     */
    public DBObject getByID(final String id, DBCollection collection) throws BadRequestException { 
        if (id == null) {
            throw new BadRequestException("Query by id the passed id was null.");
        }
        
        BasicDBObject query = new  BasicDBObject().append(DocumentUtil.TAG_ID, id);
        DBObject doc = collection.findOne(query);
        logger.trace("Query: {} Result: {}", query, doc);
        return doc;
    }
}
