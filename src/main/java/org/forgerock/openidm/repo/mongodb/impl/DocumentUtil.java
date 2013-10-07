package org.forgerock.openidm.repo.mongodb.impl;

import org.forgerock.openidm.objset.ConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for handling and converting OrientDB ODocuments
 * 
 * @author takao-s
 */
public class DocumentUtil  {
    final static Logger logger = LoggerFactory.getLogger(DocumentUtilTest.class);
    
    // Identifiers in the object model. 
    // TDOO: replace with common definitions of these global variables 
    public final static String TAG_ID = "_id";
    public final static String TAG_REV = "_rev";
    public final static String MONGODB_PRIMARY_KEY = "_openidm_id"; 

    /**
     * Parse an OpenIDM revision into an MongoDB MVCC version. MongoDB expects these to be ints.
     * @param revision the revision String with the MongoDB version in it.
     * @return the MongoDB version
     * @throws ConflictException if the revision String could not be parsed into the int expected by MongoDB
     */
    public static int parseVersion(String revision) throws ConflictException { 
        int ver = -1;
        try {
            ver = Integer.parseInt(revision);
        } catch (NumberFormatException ex) {
            throw new ConflictException("MongoDB repository expects revisions as int, " 
                    + "unable to parse passed revision: " + revision);
        }
        return ver;
    }
}