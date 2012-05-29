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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.neo4j.rest.graphdb.RequestResult;
import org.neo4j.rest.graphdb.RestAPI;

/**
 * User: KBurchardi
 * Date: 19.10.11
 * Time: 17:39
 */
public class ServiceInvocation implements RemoteInvocationStrategy{

    private RestAPI restAPI;
    private String baseUri;
    private Class<?> targetClass;

    public ServiceInvocation(RestAPI restAPI, Class<?> targetClass, String baseUri) {
        this.restAPI = restAPI;
        this.targetClass = targetClass;
        this.baseUri = baseUri;
    }

    @Override
    public RequestResult invoke(Method method, Object[] args) {

       return RequestType.determineRequestType(method).makeRequest(createUri(method, args), getRequestParams(method, args), this.restAPI.getRestRequest());
    }


    private String getClassIdentifier(){
        String identifier;
        if (targetClass.isAnnotationPresent(Path.class)){
          identifier= targetClass.getAnnotation(Path.class).value();
        }else{
          identifier= "/"+targetClass.getSimpleName();
        }

        return identifier;
    }

    private String getMethodPathIdentifier(Method method){
        if (method.isAnnotationPresent(Path.class)){
             return method.getAnnotation(Path.class).value();
        }

        throw new IllegalArgumentException("missing @Path annotation on method "+method.getName());
    }

    private String createUri(Method method, Object[] args){
        String uri = this.baseUri;
        uri+= getClassIdentifier();
        uri+= getMethodPathIdentifier(method);
        uri = replaceMethodPathWithActualValue(method, uri, args);
        return uri;
    }

    private String replaceMethodPathWithActualValue(Method method, String uri, Object[] args){
        String newUri = uri;
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        Class[] parameterTypes = method.getParameterTypes();


        for (int i=0; i <parameterTypes.length; i++){
            for(Annotation annotation : parameterAnnotations[i]){
                if(annotation instanceof PathParam){
                    PathParam pathParam = (PathParam) annotation;
                    newUri = uri.replace("{"+pathParam.value()+"}", args[i].toString());
                }
            }
        }

        return newUri;
    }
    //TODO consider @FormParam and @QueryParam
    private Map<String,Object> getRequestParams(Method method, Object[] args){
        Map<String,Object> requestParams = new HashMap<String, Object>(args.length);
        Class<?>[] paramTypes =  method.getParameterTypes();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i=0; i <paramTypes.length; i++){
           for(Annotation annotation : parameterAnnotations[i]){
                if(annotation instanceof PathParam){
                    PathParam pathParam = (PathParam) annotation;
                    requestParams.put(pathParam.value(), args[i]);
                }
            }

        }
        return requestParams;
    }
}
