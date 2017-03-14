package org.neo4j.twitter.stream;

import com.google.gson.Gson;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

/**
 * @author mh
 * @since 14.03.17
 */
class TwitterNeo4jWriter {
    static String STATEMENT = "UNWIND {tweets} AS t\n" +
            "WITH t,\n" +
            "     t.entities AS e,\n" +
            "     t.user AS u,\n" +
            "     t.retweeted_status AS retweet\n" +
            "WHERE t.id is not null " +
            "MERGE (tweet:Tweet {id:t.id})\n" +
            "SET tweet.text = t.text,\n" +
            "    tweet.created = t.created_at,\n" +
            "    tweet.favorites = t.favorite_count\n" +
            "MERGE (user:User {screen_name:u.screen_name})\n" +
            "SET user.name = u.name,\n" +
            "    user.location = u.location,\n" +
            "    user.followers = u.followers_count,\n" +
            "    user.following = u.friends_count,\n" +
            "    user.statuses = u.statuses_count,\n" +
            "    user.profile_image_url = u.profile_image_url\n" +
            "MERGE (user)-[:POSTED]->(tweet)\n" +
            "FOREACH (h IN e.hashtags |\n" +
            "  MERGE (tag:Tag {name:LOWER(h.text)})\n" +
            "  MERGE (tag)<-[:TAGGED]-(tweet)\n" +
            ")\n" +
            "FOREACH (u IN [u IN e.urls WHERE u.expanded_url IS NOT NULL] |\n" +
            "  MERGE (url:Link {url:u.expanded_url})\n" +
            "  MERGE (tweet)-[:LINKED]->(url)\n" +
            ")\n" +
            "FOREACH (m IN e.user_mentions |\n" +
            "  MERGE (mentioned:User {screen_name:m.screen_name})\n" +
            "  ON CREATE SET mentioned.name = m.name\n" +
            "  MERGE (tweet)-[:MENTIONED]->(mentioned)\n" +
            ")\n" +
            "FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |\n" +
            "  MERGE (reply_tweet:Tweet {id:r})\n" +
            "  MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)\n" +
            ")\n" +
            "FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |\n" +
            "    MERGE (retweet_tweet:Tweet {id:retweet_id})\n" +
            "    MERGE (tweet)-[:RETWEETED]->(retweet_tweet)\n" +
            ")";
    private Driver driver;

    public TwitterNeo4jWriter(String neo4jUrl) throws URISyntaxException {
        URI boltUri = new URI(neo4jUrl);
        String[] authInfo = boltUri.getUserInfo().split(":");
        driver = GraphDatabase.driver(boltUri, AuthTokens.basic(authInfo[0], authInfo[1]));
    }

    public void init() {
        try (Session session = driver.session()) {
            session.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (t:Tag) ASSERT t.name IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE");
        }
    }

    public void close() {
        driver.close();
    }

    public int insert(List<String> tweets, int retries) {
        while (retries > 0) {
            try (Session session = driver.session()) {
                Gson gson = new Gson();
                List<Map> statuses = tweets.stream().map((s) -> gson.fromJson(s, Map.class)).collect(toList());
                long time = System.nanoTime();

                ResultSummary result = session.run(STATEMENT, Values.parameters("tweets", statuses)).consume();
                int created = result.counters().nodesCreated();

                System.out.println(created+" in "+ TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-time)+" ms");
                System.out.flush();

                return created;
            } catch (Exception e) {
                System.err.println(e.getClass().getSimpleName() + ":" + e.getMessage()+" retries left "+retries);
                retries--;
            }
        }
        return -1;
    }

}
