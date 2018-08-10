package recommender;

import java.io.Console;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class DirectRetrieval
{

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Procedure(value = "recommender.directRetrieval")
    @Description("Returns recommendations using direct retrieval.")
    public Stream<Recommendation> directRetrieval( @Name("user_id") String user_id)
    {
        String query = "MATCH (u:User {id:{user_id}})-[:reviewed]->(ub:Book) " +
                        "WITH u, COLLECT(ub) AS bought_books " +
                        "MATCH (u)-->(:Book)-[r]->(b:Book) " +
                        "WHERE NOT b IN bought_books " +
                        "RETURN distinct  b, sum(r.weight) AS sum_weight " +
                        "ORDER BY sum_weight DESC LIMIT 50";
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("user_id", user_id);
        Result result = db.execute(query, params);
        return result.stream().map(Recommendation::new);
    }

    public static class Recommendation
    {
        public Node n;

        public Recommendation( Map<String, Object> map )
        {
            try {
                this.n = (Node) map.get("b");
            } catch (NullPointerException e) {
                this.n = null;
            }
        }
    }

}
