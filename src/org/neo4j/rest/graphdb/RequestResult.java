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
package org.neo4j.rest.graphdb;

import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.StatusType;

import org.neo4j.rest.graphdb.batch.RestOperations.RestOperation;
import org.neo4j.rest.graphdb.util.JsonHelper;

import com.sun.jersey.api.client.ClientResponse;


/**
* @author Klemens Burchardi
* @since 03.08.11
*/
public class RequestResult {
    private final int status;
    private final String location;
    private final String entity;
    private long batchId;
    private boolean batchResult = false;

    
    RequestResult(int status, String location, String entity) {
        this.status = status;
        this.location = location;
        this.entity = entity;
    }
    
    RequestResult(long batchId) {
       this(0,null,"");
       this.batchResult = true;
       this.batchId = batchId;
    }
    
    public static RequestResult batchResult(RestOperation restOperation){
        return new RequestResult(restOperation.getBatchId());
    }

    public static RequestResult extractFrom(ClientResponse clientResponse) {
        final int status = clientResponse.getStatus();
        final URI location = clientResponse.getLocation();
        final String data = status != Response.Status.NO_CONTENT.getStatusCode() ? clientResponse.getEntity(String.class) : null;
        clientResponse.close();
        return new RequestResult(status, uriString(location), data);
    }

    private static String uriString(URI location) {
        return location==null ? null : location.toString();
    }


    public int getStatus() {
        return status;
    }

    public String getLocation() {
        return location;
    }

    public String getEntity() {
        return entity;
    }

    public Object toEntity() {
        return JsonHelper.jsonToSingleValue( getEntity() );        
    }

    public Map<?, ?> toMap() {
        final String json = getEntity();
        return JsonHelper.jsonToMap(json);
    }

    public boolean statusIs( StatusType status ) {
        return getStatus() == status.getStatusCode();
    }

    public boolean statusOtherThan( StatusType status ) {
        return !statusIs(status );
    }
    
    public long getBatchId() {
        return batchId;
    }
    
    public boolean isBatchResult(){
        return batchResult;
    }

    public static RequestResult extractFrom(Map<String, Object> batchResult) {
        return new RequestResult(200, (String) batchResult.get("location"),JsonHelper.createJsonFrom(batchResult.get("body")));
    }
}