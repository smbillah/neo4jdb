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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.neo4j.rest.graphdb.RequestResult;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.converter.ResultTypeConverter;
import org.neo4j.rest.graphdb.converter.TypeInformation;
import org.neo4j.rest.graphdb.util.JsonHelper;

/**
 * User: KBurchardi
 * Date: 13.10.11
 * Time: 14:03
 */
public class RestInvocationHandler implements InvocationHandler{

    private final Class<?> type;
    private RestAPI restAPI;
    private RemoteInvocationStrategy invocationStrategy;
    private ResultTypeConverter resultTypeConverter;

    public RestInvocationHandler(Class<?> type, RestAPI restAPI, RemoteInvocationStrategy invocationStrategy) {
        this.type = type;
        this.restAPI = restAPI;
        this.resultTypeConverter = new ResultTypeConverter(this.restAPI);
        this.invocationStrategy = invocationStrategy;
    }



    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!type.isAssignableFrom(method.getDeclaringClass())) {
            System.out.println("method = " + method);
            return method.invoke(this,args);
        }
        final RequestResult requestResult = invocationStrategy.invoke(method,args);
        final int status = requestResult.getStatus();
        if (status!=200) throw new RuntimeException(requestResult.getEntity());
        Object obj = JsonHelper.readJson(requestResult.getEntity());
        TypeInformation typeInfo = new TypeInformation( method.getGenericReturnType());
        return this.resultTypeConverter.convertToResultType(obj, typeInfo);

    }


   public static <T> T getInvocationProxy(Class<T> type, RestAPI restAPI, RemoteInvocationStrategy invocationStrategy){
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[]{type}, new RestInvocationHandler(type,restAPI, invocationStrategy));
   }




}
