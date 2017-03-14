# neo4j-twitter-stream

Example Application to consume a twitter stream with Neo4j

## Environment variables:

* `TWITTER_TERMS` -> comma separated list of terms to listen for `happy,sad` remember to have enough volume on these, e.g. trending topics
* `TWITTER_KEYS` -> colon separated list of twitter keys: `consumerKey:consumerSecret:authToken:authSecret`
* `NEO4J_URL` -> bolt://user:pass@host:port, e.g. bolt://neo4j:****@localhost:7678

## Usage

```
export TWITTER_TERMS="happy,sad"
export TWITTER_KEYS="consumerKey:consumerSecret:authToken:authSecret"
export NEO4J_URL="bolt://neo4j:****@localhost:7678"
java org.neo4j.twitter.stream.TwitterStreamProcessor
```
