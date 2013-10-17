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

import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.objset.ConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * A utility class for handling and converting MongoDB DBObject.
 * 
 * @author takao-s
 */
public class DocumentUtil  {
    final static Logger logger = LoggerFactory.getLogger(DocumentUtil.class);
    
    // Identifiers in the object model. 
    // TDOO: replace with common definitions of these global variables 
    public final static String TAG_ID = "_id";
    public final static String TAG_REV = "_rev";
    public final static String MONGODB_PRIMARY_KEY = "_openidm_id"; 

    static final String[] KEYS_OF_STRING = {"password", "securityAnswer"};

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
    
    /**
     * MongoDB can't store data which has key starting with "$".
     * So, When key starts with "$", convert "$" to "_$".
     * 
     * @param obj
     * @return
     */
    public static DBObject normalizeForWrite (DBObject obj) {
        String regex = "\"(\\$[^\"]+)\"";
        String replacement = "\"_$1\"";
        for (int i = 0; i < KEYS_OF_STRING.length; i++) {
            if (obj.containsField(KEYS_OF_STRING[i])) {
                JsonValue jv = new JsonValue(obj.get(KEYS_OF_STRING[i]));
                String v = jv.toString().replaceAll(regex, replacement);
                obj.put(KEYS_OF_STRING[i], JSON.parse(v));
            }
        }
        return obj;
    }
    
    /**
     * unescape "_$" to "$".
     * And Json parameter convert to Json formatted String paramter,
     * Because, OpenIDM expected to String of Json format, not native Json.
     * 
     * @param obj
     * @return
     */
    public static DBObject normalizeForRead (DBObject obj) {
        String regex = "\"([^\"]+)\"";
        String replacement = "\\\"$1\\\"";
        for (int i = 0; i < KEYS_OF_STRING.length; i++) {
            if (obj.containsField(KEYS_OF_STRING[i])) {
                JsonValue jv = new JsonValue(obj.get(KEYS_OF_STRING[i]));
                String v = jv.toString().replaceAll(regex, replacement);
                v = v.replaceAll(" ", "");
                v = v.replaceFirst("\"_\\$", "\"\\$");
                obj.put(KEYS_OF_STRING[i], v);
            }
        }
        return obj;
    }
}