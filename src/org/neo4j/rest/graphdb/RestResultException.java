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

import java.util.List;
import java.util.Map;

public class RestResultException extends RuntimeException {
    public RestResultException(Object result) {
        super(format(toMap(result)));
    }

    private static String format(Map<?, ?> result) {
        if (result==null) return "Unknown Exception";
        StringBuilder sb = new StringBuilder();
        sb.append(result.get("message")).append(" at\n");
        sb.append(result.get("exception")).append("\n");
        List<String> stacktrace = (List<String>) result.get("stacktrace");
        if (stacktrace != null) {
            for (String line : stacktrace) {
                sb.append("   ").append(line).append("\n");
            }
        }
        return sb.toString();
    }

    public static boolean isExceptionResult(Object result) {
        final Map<String, Object> map = toMap(result);
        return map!=null && map.containsKey("exception") && map.containsKey("message");
    }

    private static Map<String, Object> toMap(Object result) {
        if (!(result instanceof Map)) return null;
        return (Map<String, Object>) result;

    }
}
