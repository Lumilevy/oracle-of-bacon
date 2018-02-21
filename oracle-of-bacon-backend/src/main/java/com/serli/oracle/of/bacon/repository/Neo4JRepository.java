package com.serli.oracle.of.bacon.repository;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "florinecercle"));
    }

    public List<GraphItem> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();

        // TODO implement Oracle of Bacon
        Transaction t = session.beginTransaction();
        Statement statement = new Statement("MATCH (ee:ACTORS {name: 'Bacon, Kevin (I)'}), (ff:ACTORS {name: '"
                + actorName + "'}), p = shortestPath((ee)-[*]-(ff)) return p");
        StatementResult result = t.run(statement);
        return fromListToGraph(result.list());
    }

    public List<GraphItem> fromListToGraph(List<Record> list) {
        Path p = list.get(0).get(0).asPath();
        Iterable<Node> iterableNode = p.nodes();
        List<GraphItem> graphItemList = new ArrayList<GraphItem>();
        for (Node n : iterableNode) {
            GraphNode node = new GraphNode(n.id(), n.values().iterator().next().toString(),
                    n.labels().iterator().next());
            graphItemList.add(node);
        }
        Iterable<Relationship> iterableRelationship = p.relationships();
        for(Relationship r : iterableRelationship){
            GraphEdge edge = new GraphEdge(r.id(), r.startNodeId(), r.endNodeId(), r.type());
            graphItemList.add(edge);
        }
        System.out.println(graphItemList);
        return graphItemList;
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public String getValue() {
            return value;
        }


    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }

        public long getSource() {
            return source;
        }

        public long getTarget() {
            return target;
        }

        public String getValue() {
            return value;
        }
    }
}
