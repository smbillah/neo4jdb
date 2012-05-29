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
package org.neo4j.rest.graphdb.query;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.rest.graphdb.RequestResult;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestRequest;
import org.neo4j.rest.graphdb.RestResultException;
import org.neo4j.rest.graphdb.converter.RestEntityExtractor;
import org.neo4j.rest.graphdb.converter.RestTableResultExtractor;
import org.neo4j.rest.graphdb.util.ConvertedResult;
import org.neo4j.rest.graphdb.util.DefaultConverter;
import org.neo4j.rest.graphdb.util.Handler;
import org.neo4j.rest.graphdb.util.QueryResult;
import org.neo4j.rest.graphdb.util.QueryResultBuilder;
import org.neo4j.rest.graphdb.util.ResultConverter;

public class RestCypherQueryEngine implements QueryEngine<Map<String,Object>> {
    private final RestRequest restRequest;
    private final RestAPI restApi;
    private final ResultConverter resultConverter;

    public RestCypherQueryEngine(RestAPI restApi) {
        this(restApi,null);
    }
    public RestCypherQueryEngine(RestAPI restApi, ResultConverter resultConverter) {
        this.restApi = restApi;
        this.resultConverter = resultConverter!=null ? resultConverter : new DefaultConverter();
        this.restRequest = restApi.getRestRequest();
    }
    
    @Override
    public QueryResult<Map<String, Object>> query(String statement, Map<String, Object> params) {
        final RequestResult requestResult = restRequest.get("ext/CypherPlugin/graphdb/execute_query", MapUtil.map("query", statement, "params", params));
        final Map<?, ?> resultMap = restRequest.toMap(requestResult);
        if (RestResultException.isExceptionResult(resultMap)) throw new RestResultException(resultMap);
        return new RestQueryResult(resultMap,restApi,resultConverter);
    }


    public static class RestQueryResult implements QueryResult<Map<String,Object>> {
        QueryResultBuilder<Map<String,Object>> result;

        @Override
        public <R> ConvertedResult<R> to(Class<R> type) {
            return result.to(type);
        }

        @Override
        public <R> ConvertedResult<R> to(Class<R> type, ResultConverter<Map<String, Object>, R> converter) {
            return result.to(type,converter);
        }

        @Override
        public void handle(Handler<Map<String, Object>> handler) {
            result.handle(handler);
        }

        @Override
        public Iterator<Map<String, Object>> iterator() {
            return result.iterator();
        }

        public RestQueryResult(Map<?, ?> result, RestAPI restApi, ResultConverter resultConverter) {
            final RestTableResultExtractor extractor = new RestTableResultExtractor(new RestEntityExtractor(restApi));
            final List<Map<String, Object>> data = extractor.extract(result);
            this.result=new QueryResultBuilder<Map<String,Object>>(data, resultConverter);
        }
    }
}