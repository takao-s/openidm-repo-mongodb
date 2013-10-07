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