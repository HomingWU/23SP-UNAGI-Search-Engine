# 23sp-CIS5550-Team-unagi

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
