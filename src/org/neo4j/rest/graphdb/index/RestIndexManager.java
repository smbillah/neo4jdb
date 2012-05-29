/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.rest.graphdb.index;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestRequest;

public class RestIndexManager implements IndexManager {
    public static final String RELATIONSHIP = "relationship";
    public static final String NODE = "node";
    private RestRequest restRequest;
    private RestAPI restApi;

    public RestIndexManager( RestRequest restRequest, RestAPI restApi ) {
        this.restRequest = restRequest;
        this.restApi = restApi;
    }

    public boolean existsForNodes( String indexName ) {
        return indexInfo(NODE).exists(indexName);
    }

    @SuppressWarnings({"unchecked"})
    private IndexInfo indexInfo(final String indexType) {
    	return restApi.indexInfo(indexType);
    }
    
    @SuppressWarnings("unchecked")
	private boolean checkIndex(  final String indexType, final String indexName, Map<String, String> config ){
        final IndexInfo indexInfo = indexInfo(indexType);
        return indexInfo.checkConfig(indexName, config);
    }

    public boolean noConfigProvided(Map<String,String> config) {
    	return config == null || config.isEmpty();
    }

    public RestIndex<Node> forNodes( String indexName ) {
    	if (!checkIndex(NODE, indexName, null)){    		
    		createIndex(NODE, indexName,  LuceneIndexImplementation.EXACT_CONFIG);
    	}
        return new RestNodeIndex( restRequest, indexName, restApi );
    }

    public RestIndex<Node> forNodes( String indexName, Map<String, String> config ) {
    	if (noConfigProvided(config)){
    		throw new IllegalArgumentException("No index configuration was provided!");
    	}
    	if (!checkIndex(NODE, indexName, config)){
    		createIndex(NODE, indexName, config);
    	}    	
        return new RestNodeIndex( restRequest, indexName, restApi );
    }

    public String[] nodeIndexNames() {
        final IndexInfo indexInfo = indexInfo(NODE);
        return indexInfo.indexNames();
    }

    public boolean existsForRelationships( String indexName ) {
        return indexInfo(RELATIONSHIP).exists(indexName);
    }

    public RelationshipIndex forRelationships( String indexName ) {
    	if (!checkIndex(RELATIONSHIP, indexName, null)){    		
    		createIndex(RELATIONSHIP, indexName,  LuceneIndexImplementation.EXACT_CONFIG);
    	}
        return new RestRelationshipIndex( restRequest, indexName, restApi );
    }

    public RelationshipIndex forRelationships( String indexName, Map<String, String> config ) {
    	if (noConfigProvided(config)){
    		throw new IllegalArgumentException("No index configuration was provided!");
    	}
    	if (!checkIndex(RELATIONSHIP, indexName, config)){
    		createIndex(RELATIONSHIP, indexName, config);
    	}    
        return new RestRelationshipIndex( restRequest, indexName, restApi );
    }

    private void createIndex(String type, String indexName, Map<String, String> config) {
        Map<String,Object> data=new HashMap<String, Object>();
        data.put("name",indexName);
        data.put("config",config);
        restRequest.post("index/" + type, data);
    }

    public String[] relationshipIndexNames() {
        return indexInfo(RELATIONSHIP).indexNames();
    }

    @SuppressWarnings({"unchecked"})
    public Map<String, String> getConfiguration( Index<? extends PropertyContainer> index ) {
        String typeName = typeName(index.getEntityType());
        return indexInfo(typeName).getConfig(index.getName());
    }

    private String typeName(Class<? extends PropertyContainer> type) {
        if (Node.class.isAssignableFrom(type)) return NODE;
        if (Relationship.class.isAssignableFrom(type)) return RELATIONSHIP;
        throw new IllegalArgumentException("Invalid index type "+type);
    }

    public String setConfiguration( Index<? extends PropertyContainer> index, String s, String s1 ) {
        throw new UnsupportedOperationException();
    }

    public String removeConfiguration( Index<? extends PropertyContainer> index, String s ) {
        throw new UnsupportedOperationException();
    }

	@Override
	public AutoIndexer<Node> getNodeAutoIndexer() {
		 throw new UnsupportedOperationException();
	}

	@Override
	public RelationshipAutoIndexer getRelationshipAutoIndexer() {
		 throw new UnsupportedOperationException();
	}
}

