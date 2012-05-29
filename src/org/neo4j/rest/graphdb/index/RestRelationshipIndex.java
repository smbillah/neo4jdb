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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestRequest;

/**
 * @author mh
 * @since 24.01.11
 */
public class RestRelationshipIndex extends RestIndex<Relationship> implements RelationshipIndex {
    public RestRelationshipIndex( RestRequest restRequest, String indexName, RestAPI restApi ) {
        super( restRequest, indexName, restApi );
    }

    public Class<Relationship> getEntityType() {
        return Relationship.class;
    }

    public org.neo4j.graphdb.index.IndexHits<Relationship> get( String s, Object o, Node node, Node node1 ) {
        throw new UnsupportedOperationException();
    }

    public org.neo4j.graphdb.index.IndexHits<Relationship> query( String s, Object o, Node node, Node node1 ) {
        throw new UnsupportedOperationException();
    }

    public org.neo4j.graphdb.index.IndexHits<Relationship> query( Object o, Node node, Node node1 ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWriteable() {       
        return true;
    }
}
