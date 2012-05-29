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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * @author mh
 * @since 25.01.11
 */
public class GraphDatabaseFactory {
    public static GraphDatabaseService databaseFor(String url) {
        return databaseFor( url, null,null );
    }

    public static GraphDatabaseService databaseFor(String url, String username, String password) {
        if (url.startsWith( "http://" ) || url.startsWith( "https://" )) {
            return new RestGraphDatabase(  url , username,password );
        }
        String path=url;
        if (url.startsWith( "file:" )) {
            path = toURI( url ).getPath();
        }
        File file = new File( path );
        if (!file.isDirectory()) file=file.getParentFile();
        return new EmbeddedGraphDatabase( file.getAbsolutePath() );
    }

    private static URI toURI( String uri ) {
        try {
            return new URI(uri);
        } catch ( URISyntaxException e ) {
            throw new RuntimeException( "Error using URI "+uri, e);
        }
    }
}
