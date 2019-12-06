MATCH (n)-[r:DISABLED_RATES]->(m)
MERGE (n)-[new:RATES]->(m)
SET new = r
WITH r
DELETE r;

MATCH (n)-[r:PROBABLY_LIKES_IB]->(m)
DELETE r;

MATCH (n)-[r:PROBABLY_LIKES_UB]->(m)
DELETE r;

MATCH (n)-[r:PROBABLY_LIKES_RW|:PROBABLY_LIKES_BRW]->(m)
DELETE r;

MATCH (n)-[r:PROBABLY_LIKES_BRW]->(m)
DELETE r;

MATCH (n:User {testUser: true})
SET n.testUser = false;
