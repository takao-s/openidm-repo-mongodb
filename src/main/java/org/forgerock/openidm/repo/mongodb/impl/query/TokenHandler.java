/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright © 2011 ForgeRock AS. All rights reserved.
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

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.forgerock.openidm.objset.ObjectSetException;
import org.forgerock.openidm.objset.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Token handler of query
 * 
 * @author takao-s
 */
public class TokenHandler {
    public static final String PREFIX_UNQUOTED = "unquoted";

    public static final String PREFIX_DOTNOTATION = "dotnotation";

    public static final String PREFIX_FIELDS = "fields";

    final static Logger logger = LoggerFactory.getLogger(TokenHandler.class);
    
    // The OpenIDM query token is of format "${token-name}"
    Pattern tokenPattern = Pattern.compile("\"?\\$\\{(.+?)\\}\"?");

    /**
     * Replaces a query string with tokens of format "${token-name}" with the values from the
     * passed in map, where the token-name must be the key in the map
     * 
     * @param queryString the query with tokens
     * @param params the parameters to replace the tokens. Values can be String or List.
     * @return the query with all tokens replace with their found values
     * @throws BadRequestException if token in the query is not in the passed parameters
     */
    String replaceTokensWithValues(String queryString, Map<String, Object> params) 
            throws BadRequestException {
        Matcher matcher = tokenPattern.matcher(queryString);
        StringBuffer buffer = new StringBuffer();
        String quote = "\"";
        String fields_option = "";
        while (matcher.find()) {
            String fullTokenKey = matcher.group(1);
            String tokenKey = fullTokenKey;
            String tokenPrefix = null;
            String[] tokenKeyParts = tokenKey.split(":", 2);
            // if prefix found
            if (tokenKeyParts.length == 2) {
                tokenPrefix = tokenKeyParts[0];
                tokenKey = tokenKeyParts[1];
            }
            if (!params.containsKey(tokenKey)) {
                // fail with an exception if token not found
                throw new BadRequestException("Missing entry in params passed to query for token " + tokenKey);
            } else {
                Object replacement = params.get(tokenKey);
                
                if (PREFIX_UNQUOTED.equals(tokenPrefix)) {
                    quote = "";
                }
                if (PREFIX_FIELDS.equals(tokenPrefix)) {
                    fields_option = ":true";
                }
                if (replacement instanceof List) {
                    StringBuffer commaSeparated = new StringBuffer();
                    boolean first = true;
                    for (Object entry : ((List) replacement)) {
                        if (!first) {
                            commaSeparated.append(quote + fields_option + "," + quote);
                        } else {
                            first = false;
                        }
                        commaSeparated.append(entry.toString());
                    }
                    replacement = commaSeparated.toString();
                }
                if (replacement == null) {
                    replacement = "";
                }
                
                // Optional control of representation via prefix
                if (PREFIX_DOTNOTATION.equals(tokenPrefix)) {
                    // Convert Json Pointer to OrientDB dot notation
                    String dotDelimited = replacement.toString().replace('/', '.');
                    if (dotDelimited.startsWith(".")) {
                        replacement = dotDelimited.substring(1);
                    } else {
                        replacement = dotDelimited;
                    }
                }
                replacement = quote + replacement + quote + fields_option;
                if (PREFIX_FIELDS.equals(tokenPrefix)) {
                    replacement = "{" + replacement +"}";
                }
                matcher.appendReplacement(buffer, "");
                buffer.append(replacement);
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }
}