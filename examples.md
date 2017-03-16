MATCH (t:Tweet)–[:LINKED]->(l:Link)
WITH l, count(*) as c order by c DESC LIMIT 10
RETURN l.url, c


MATCH (u1:User)-[:POSTED]->(t:Tweet)–[:MENTIONED]->(u2:User),(t)-[:TAGGED]->(tag:Tag)
WITH u1,u2, count(*) as times,collect(distinct tag) as tags
ORDER BY times DESC LIMIT 100
RETURN u1.screen_name,u2.screen_name, times, [t IN tags| t.name][0..10] as tagNames


MATCH (u:User)–[p:POSTED]->(t:Tweet)
WITH u, count(p) as rel_count 
ORDER BY rel_count DESC
RETURN u LIMIT 10;

MATCH (u:User)
RETURN u, size( (u)–[:POSTED]->() )  as tweets 
ORDER BY tweets DESC LIMIT 10;
