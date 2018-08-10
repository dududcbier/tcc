package recommender;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.parameters;
public class DirectRetrievalTest
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure( DirectRetrieval.class );

    @Test
    public void shouldRecommendAlsoBoughtBooks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            StatementResult r = session.run( "CREATE (p:User {name:'Teste', id:'1'})-[:reviewed]->(:Book)-[:also_bought]->(r:Book) RETURN p.id, r" );
            Record record = r.next();
            String userId = record.get("p.id").asString();
            Node recommendation = record.get("r").asNode();

            Node res = session.run( "CALL recommender.directRetrieval({id})", parameters( "id", userId ) ).single().get(0).asNode();

            assertThat(recommendation, equalTo(res));
        }
    }

    @Test
    public void shouldRecommendAlsoViewedBooks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            StatementResult r = session.run( "CREATE (p:User {name:'Teste', id:'1'})-[:reviewed]->(:Book)-[:also_viewed]->(r:Book) RETURN p.id, r" );
            Record record = r.next();
            String userId = record.get("p.id").asString();
            Node recommendation = record.get("r").asNode();

            Node res = session.run( "CALL recommender.directRetrieval({id})", parameters( "id", userId ) ).single().get(0).asNode();

            assertThat(recommendation, equalTo(res));
        }
    }

    @Test
    public void shouldRecommendBuyAfterViewingBooks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            StatementResult r = session.run( "CREATE (p:User {name:'Teste', id:'1'})-[:reviewed]->(:Book)-[:buy_after_viewing]->(r:Book) RETURN p.id, r" );
            Record record = r.next();
            String userId = record.get("p.id").asString();
            Node recommendation = record.get("r").asNode();

            Node res = session.run( "CALL recommender.directRetrieval({id})", parameters( "id", userId ) ).single().get(0).asNode();

            assertThat(recommendation, equalTo(res));
        }
    }

    @Test
    public void shouldNotRecommendUnrelatedBooks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            StatementResult r = session.run( "CREATE (p:User {name:'Teste', id:'1'})-[:reviewed]->(:Book) RETURN p.id" );
            String userId = r.next().get("p.id").asString();
            r = session.run("CREATE (b:Book) RETURN b");

            r = session.run( "CALL recommender.directRetrieval({id})", parameters( "id", userId ) );
            
            assertThat(r.hasNext(), equalTo(false));
        }
    }

    @Test
    public void shouldNotRecommendIndirectBooks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            StatementResult r = session.run( "CREATE (p:User {name:'Teste', id:'1'})-[:reviewed]->(:Book)-[:buy_after_viewing]->(:Book)-[:also_bought]->(b:Book) RETURN p.id, b" );
            Record record = r.next();
            String userId = record.get("p.id").asString();
            Node indirect_book = record.get("b").asNode();

            Node res = session.run( "CALL recommender.directRetrieval({id})", parameters( "id", userId ) ).single().get(0).asNode();
           
            assertThat(res, not(equalTo(indirect_book)));
        }
    }

    @Test
    public void shouldNotRecommendBooksAlreadyBought() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            StatementResult r = session.run( "CREATE (b:Book)<-[:reviewed]-(p:User {name:'Teste', id:'1'})-[:reviewed]->(:Book)-[:also_bought]->(b) RETURN p.id" );
            String userId = r.next().get("p.id").asString();

            r = session.run( "CALL recommender.directRetrieval({id})", parameters( "id", userId ) );
            
            assertThat(r.hasNext(), equalTo(false));
        }
    }
    
}
