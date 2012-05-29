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
package org.neo4j.rest.graphdb.services;

import java.lang.reflect.Method;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;

import org.neo4j.rest.graphdb.RequestResult;
import org.neo4j.rest.graphdb.RestRequest;

/**
* User: KBurchardi
* Date: 21.10.11
* Time: 00:57
*/
enum RequestType {
    PUT {
        @Override
        RequestResult makeRequest(String uri, Map<String, Object> requestParams, RestRequest restRequest) {
           return restRequest.put(uri, requestParams);
        }
    },
    POST {
        @Override
        RequestResult makeRequest(String uri, Map<String, Object> requestParams, RestRequest restRequest) {
            return restRequest.post(uri, requestParams);
        }
    },
    GET {
        @Override
        RequestResult makeRequest(String uri, Map<String, Object> requestParams, RestRequest restRequest) {
           return restRequest.get(uri);
        }
    },
    DELETE {
        @Override
        RequestResult makeRequest(String uri, Map<String, Object> requestParams, RestRequest restRequest) {
             return restRequest.delete(uri);
        }
    };

    public static RequestType determineRequestType(Method method){
        if (method.isAnnotationPresent(GET.class)){
            return GET;
        }

        if (method.isAnnotationPresent(POST.class)){
            return POST;
        }

        if (method.isAnnotationPresent(PUT.class)){
            return PUT;
        }

        if (method.isAnnotationPresent(DELETE.class)){
            return DELETE;
        }
           throw new IllegalArgumentException("missing Annotation for the request type, e.g. @GET");
    }

    abstract RequestResult makeRequest(String uri, Map<String, Object> requestParams, RestRequest restRequest);

}
