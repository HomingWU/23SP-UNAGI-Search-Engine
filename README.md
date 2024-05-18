# 23SP-UNAGI-Search-Engine
- A Google-style search engine, building entirely from scratch using Java.
- Including components that a modern search engine would use: a web server, a key-value store, an analytics engine, a crawler, an indexer and PageRank, and a simple frontend.
- Group project of 4. I am responsible for providing all the components above and deployment on the Amazon EC2. One teammate was responsible for fine-tuning the crawler and babysitting the crawling process,
  one responsible for data processing using the analytics engine, and the last one responsible for tuning the ranking.

## Compiling:
```bash
Ranking: javac --source-path src src/cis5550/ranking/Ranking.java
Frontend: javac --source-path src src/cis5550/frontend/Frontend.java
KVSMaster: javac --source-path src src/cis5550/kvs/Master.java
KVSWorker: javac --source-path src src/cis5550/kvs/Worker.java
```

## Running:
```bash
KVSMaster: java -cp src cis5550.kvs.Master 8000 //8000 is the port number the KVSMaster listening to
KVSWorker: java -cp src cis5550.kvs.Worker 9000 0.0.0.0:8000 //9000 is the port number the KVSWorker listening to, 0.0.0.0:8000 is the KVSMaster's ip:port
Ranking: java -cp src cis5550.ranking.Ranking 9010 0.0.0.0:8000 //9010 is the port number the Ranking listening to, 0.0.0.0:8000 is the KVSMaster's ip:port
Frontend: java -cp src cis5550.frontend.Frontend 0.0.0.0 443 1.1.1.1:9010 //0.0.0.0 is the ip of the frontend server, 443 is the port number, 1.1.1.1:9010 is the ip:port of the ranking
```
