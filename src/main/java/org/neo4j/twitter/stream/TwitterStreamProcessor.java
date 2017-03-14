package org.neo4j.twitter.stream;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static java.util.Arrays.asList;

public class TwitterStreamProcessor {

    private static final int BATCH = 100;

    public static void main(String... args) throws InterruptedException, MalformedURLException, URISyntaxException {
        int maxReads = 1000000;

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        BasicClient client = configureStreamClient(msgQueue, System.getenv("TWITTER_KEYS"));
        TwitterNeo4jWriter writer = new TwitterNeo4jWriter(System.getenv("NEO4J_URL"));

        int numProcessingThreads = Math.max(1,Runtime.getRuntime().availableProcessors() - 1);
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        client.connect();

        List<String> buffer = new ArrayList<>(BATCH);
        for (int msgRead = 0; msgRead < maxReads; msgRead++) {
            if (client.isDone()) {
                System.err.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }
            String msg = msgQueue.poll(5, TimeUnit.SECONDS);
            if (msg == null) System.out.println("Did not receive a message in 5 seconds");
            else buffer.add(msg);

            if (buffer.size() < BATCH) continue;

            List<String> tweets = buffer;
            service.submit(() -> writer.insert(tweets,3));
            buffer = new ArrayList<>(BATCH);
        }


        client.stop();
        writer.close();
    }

    private static BasicClient configureStreamClient(BlockingQueue<String> msgQueue, String twitterKeys) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.followings(asList(1234L, 566788L));
        endpoint.trackTerms(asList("twitter", "api"));
        endpoint.stallWarnings(false);

        String[] keys = twitterKeys.split(":");
        Authentication auth = new OAuth1(keys[0], keys[1], keys[2], keys[3]);

        ClientBuilder builder = new ClientBuilder()
                .name("Neo4j-Twitter-Stream")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
