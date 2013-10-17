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
