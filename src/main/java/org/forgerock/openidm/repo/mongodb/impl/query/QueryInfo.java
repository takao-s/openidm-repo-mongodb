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

import java.util.List;

/**
 * Information about a query, and state of the query
 * 
 * @author takao-s
 */
final class QueryInfo {
    
    private boolean groupQuery = false;
    private String query;
    private String fileds;
    private String sort;
    private List<String> aggregationParams;

    public QueryInfo(boolean isGroupQuery) {
        this.groupQuery = isGroupQuery;
    }
    
    public boolean isGroupQuery() {
        return groupQuery;
    }
    
    public List<String> getAggregationParams() {
        return aggregationParams;
    }
    
    public void setAggregationParams(List<String> aggregationParams) {
        this.aggregationParams = aggregationParams;
    }
    
    public String getQuery() {
        return this.query;
    }
    
    public void setQuery(String query) {
        this.query = query;
    }
    
    public String getFileds() {
        return fileds;
    }
    
    public void setFileds(String fileds) {
        this.fileds = fileds;
    }
    
    public String getSort() {
        return sort;
    }
    
    public void setSort(String sort) {
        this.sort = sort;
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer();
        if (groupQuery) {
            for (String s : aggregationParams) {
                sb.append(s).append(" ");
            }
        } else {
            if (query != null) {
                sb.append(query).append(" ");
            }
            if (fileds != null) {
                sb.append(fileds).append(" ");
            }
            if (sort != null) {
                sb.append(sort).append(" ");
            }
        }
        return sb.toString();
    }
}