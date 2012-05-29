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
package org.neo4j.rest.graphdb.batch;


import java.util.Map;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.rest.graphdb.ExecutingRestRequest;
import org.neo4j.rest.graphdb.RequestResult;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestRequest;
import org.neo4j.rest.graphdb.converter.RelationshipIterableConverter;
import org.neo4j.rest.graphdb.converter.RestEntityExtractor;
import org.neo4j.rest.graphdb.converter.RestEntityPropertyRefresher;
import org.neo4j.rest.graphdb.converter.RestIndexHitsConverter;
import org.neo4j.rest.graphdb.entity.RestEntity;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;
import org.neo4j.rest.graphdb.index.IndexInfo;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.index.SimpleIndexHits;

public class BatchRestAPI extends RestAPI {

    public BatchRestAPI( String uri ) {
        super(uri);
    }

    public BatchRestAPI( String uri, String user, String password ) {       
        super(uri, user, password); 
    }
    
    public BatchRestAPI(String uri, ExecutingRestRequest executingRestRequest){
        super(uri);
        this.restRequest =  new RecordingRestRequest(executingRestRequest, new RestOperations());
    }
    
    @Override
    protected RestRequest createRestRequest( String uri, String user, String password){
        return new RecordingRestRequest(new ExecutingRestRequest(uri,  user,  password),new RestOperations());
    }
    
    
    @Override
    public RestNode createRestNode(RequestResult requestResult) {
        final long batchId = requestResult.getBatchId();
        RestNode node = new RestNode("{"+batchId+"}", this);
        (getRecordingRequest()).getOperations().addToRestOperation(batchId, node, new RestEntityExtractor(this));
        return node;
    }
          
    
    @Override
    public RestRelationship createRestRelationship(RequestResult requestResult, PropertyContainer element) {
        final long batchId = requestResult.getBatchId();
        RestRelationship relationship = new RestRelationship("{"+batchId+"}", this);
        getRecordingRequest().getOperations().addToRestOperation(batchId, relationship, new RestEntityExtractor(this));
        return relationship;
    }

    private RecordingRestRequest getRecordingRequest() {
        return (RecordingRestRequest)this.restRequest;
    }

    public RestOperations getRecordedOperations(){
       return (getRecordingRequest()).getOperations();
    }

    public void stop() {
        getRecordingRequest().stop();
    }

    @SuppressWarnings("unchecked")
    public Iterable<Relationship> wrapRelationships(  RequestResult requestResult ) {
        final long batchId = requestResult.getBatchId();
        final BatchIterable<Relationship> result = new BatchIterable<Relationship>(requestResult);
        getRecordingRequest().getOperations().addToRestOperation(batchId, result, new RelationshipIterableConverter(this));
        return result;
    }

    public <S extends PropertyContainer> IndexHits<S> queryIndex(String indexPath, Class<S> entityType) {
        RequestResult response = restRequest.get(indexPath);
        final long batchId = response.getBatchId();
        final SimpleIndexHits<S> result = new SimpleIndexHits<S>(batchId, entityType, this);
        getRecordingRequest().getOperations().addToRestOperation(batchId, result, new RestIndexHitsConverter(this,entityType));
        return result;
    }    
   

    public  IndexInfo indexInfo(final String indexType) {
        return new BatchIndexInfo();
    }
     
    @Override
    public void setPropertyOnEntity( RestEntity entity, String key, Object value ) {       
        RequestResult response = entity.getRestRequest().put( "properties/" + key, value);
        final long batchId = response.getBatchId();     
        getRecordingRequest().getOperations().addToRestOperation(batchId, entity, new RestEntityPropertyRefresher(entity));       
    }
    
    @Override
    public <T> void addToIndex( T entity, RestIndex index,  String key, Object value ) {
        final RestEntity restEntity = (RestEntity) entity;
        String uri = restEntity.getUri();
        if (value instanceof ValueContext) {
            value = ((ValueContext)value).getCorrectValue();
        }
        final Map<String, Object> data = MapUtil.map("key", key, "value", value, "uri", uri);
        final RequestResult result = restRequest.post(index.indexPath(), data);        
    }
    
    @Override
    public <T> void removeFromIndex(RestIndex index, T entity) {       
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> void removeFromIndex( RestIndex index, T entity, String key, Object value ) {
        throw new UnsupportedOperationException();
    }  
    
    @Override
    public <T> void removeFromIndex(RestIndex index, T entity, String key) {
        throw new UnsupportedOperationException();
    }
         

    private static class BatchIndexInfo implements IndexInfo {

        @Override
        public boolean checkConfig(String indexName, Map<String, String> config) {
            return true;
        }

        @Override
        public String[] indexNames() {
            return new String[0];
        }

        @Override
        public boolean exists(String indexName) {
            return true;
        }

        @Override
        public Map<String, String> getConfig(String name) {
            return null;
        }
    }
}
