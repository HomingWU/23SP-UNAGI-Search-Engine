# 23SP-UNAGI-Search-Engine
- A Google-style search engine, building entirely from scratch using Java.
- Including components that a modern search engine would use: a web server, a key-value store, an analytics engine, a crawler, an indexer and PageRank, and a simple frontend.
- Group project of 4. I am responsible for providing all the components above and deployment on the Amazon EC2. One teammate was responsible for fine-tuning the crawler and babysitting the crawling process,
  one responsible for data processing using the analytics engine, and the last one responsible for tuning the ranking.

## 1. Web Server
- An API that is based on the API from Spark Framework, a real-world framework creating web applications in Java.
- The web server supports dynamic content and routes as well as static files from a given directory.
- Key features: ***Concurrency, Security, Server API, Route API, Error handling, HTTPS support, Sessions, Session expiration.***

## 2. Key-Value Store
- A ***distributed*** key-value store with an in-memory/persistence option.
- Sharded across multiple worker nodes via consistent hashing, coordinated by a master node tracking active workers and managing system membership, with communication via HTTP.
- Key features: ***In-memory storage, Persistent tables with on-disk storage, Recovery,  Streaming read, Streaming write, Rename, Delete, User interface.***

## 3. Analytics Engine
- A ***distributed*** analytics engine that is loosely based on Apache Spark that can work with large data sets that are spread across several nodes(RDDs).
- Support basic operations, such as flatMap, fold, and join, to normal RDDs and Pair RDDs. 
- Key features: ***Job submission, Parallelism, Encoding, User interface***

## 4. Distributed Web Crawler
- A ***distributed*** web crawler, based on the analytics engine and KVS.
- The crawler can follow redirects, and comply with robot exclusion protocol and craw delay policy.
- Key features: ***Robot exclusion protocol, User agent string, Crawl-delay, URL extraction and normalization, URL filtering, Redirect handling.***

## 5. Indexer and PageRank
- An indexer can build an inverted index that maps each word to a list of pages that contains it.
- The implementation of the PageRank algorithm extracts the link graph from the crawled pages and runs the algorithm on that graph.
- Key features: ***Word positions, Convergence criterion, Stemming***
  
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
