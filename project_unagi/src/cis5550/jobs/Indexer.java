package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Stemmer;
import cis5550.webserver.Server;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

import java.util.*;

import static java.lang.Math.min;

public class Indexer {
    private static final Logger logger = Logger.getLogger(Server.class);
    static Hasher hasher = new Hasher();

    public static void main(String[] args) {
        logger.info("main seed url ");
    }

    public static void run(FlameContext flameContext, String[] args) throws Exception {

        KVSClient kvsClient = flameContext.getKVS();

        FlameRDD urls = flameContext.fromTable("crawl", r -> {
                    if (r.get("url") == null) return null;
                    if (r.get("page") == null) return null;
                    if (r.get("url").length() > 1000) return null;
                    if (r.get("url").contains(",")) return null;
                    if (r.get("url").contains("..")) return null;
                    if (r.get("page") != null && r.get("page").length() > 100) {
                        Document doc = Jsoup.parse(r.get("page"));
                        String body = wordTrim(doc.body().text());
                        String title = wordTrim(doc.title());
                        String substring = body.substring(0, min(100, body.length()));
                        String[] splits = body.replaceAll("[^a-zA-Z0-9]", " ").split("\\s");
                        String urlHash = Hasher.hash(r.get("url"));
                        try {
                            flameContext.getKVS().persist("headers");
                            flameContext.getKVS().put("headers", urlHash, "Context", r.get("url") + ",title: " + title + ", firstWords: " + substring + ", total_words: " + splits.length);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return urlHash + ",page: " + title + " " + body;
                    }
                    return null;
                }
        );
        FlamePairRDD map = urls.mapToPair(p -> {
                    if (p == null) return null;
                    String[] splits = p.split(",page:");
                    try {
                        if (splits.length > 1) {
                            if (splits[0] != null && splits[1] != null)
                                return new FlamePair(splits[0], splits[1]);
                        }
                    } catch (Exception e) {
                        logger.info(p);
                        return null;
                    }
                    return null;
                }
        );
        urls.saveAsTable("urls");
        kvsClient.delete("urls");
        FlamePairRDD flamePairRDD = map.flatMapToPair(p -> {
            if (p == null) return null;
            logger.info("join: " + p._1());
            HashMap<String, String> results = new HashMap<>();
            ArrayList<FlamePair> entries = new ArrayList<>();

            String[] splits = p._2().replaceAll("<.*?>", " ").replaceAll("[^a-zA-Z0-9]", " ").toLowerCase().split("\\s");
            if (splits.length < 10) return entries;
            int i = 1;
            for (String entry : splits) {
                if (!entry.equals("") && !entries.contains(entry)) {
                    String newWord = wordStem(entry);
                    if (entry.length() > 10) continue;
                    if (entry.matches("[0-9]+") && (entry.length() != 4)) {
                        continue;
                    }
                    if (entry.matches("[0-9]+[a-zA-z]+")) continue;
                    if (entry.matches(".*\\d+.*") && !entry.matches("[0-9]+")) continue;
                    if (!flameContext.getKVS().existsRow("dictionary", entry) && !flameContext.getKVS().existsRow("dictionary", newWord))
                        continue;
                    if (!results.containsKey(entry)) {
                        results.put(entry, p._1() + ":" + i);
                    } else {
                        results.put(entry, results.get(entry) + " " + i);
                    }
                    if (!newWord.equals(entry)) {
                        if (!results.containsKey(newWord)) {
                            results.put(newWord, p._1() + ":" + i);
                        } else {
                            results.put(newWord, results.get(newWord) + " " + i);
                        }
                    }
                    i++;
                }
                if (results.size()> 100){
                    break;
                }
            }
            for (Map.Entry<String, String> entry : results.entrySet()) {
                entries.add(new FlamePair(entry.getKey(), entry.getValue()));
            }
            return entries;
        });

        FlamePairRDD flamePairRDD1 = flamePairRDD.foldByKey("", (String a, String b) -> {
            if (!a.equals("")) {
                return a + "," + b;
            }
            return b;
        });
        flamePairRDD.saveAsTable("assignWord");
        kvsClient.delete("assignWord");

        FlamePairRDD result = flamePairRDD1.flatMapToPair(p -> {
            List<FlamePair> ret = new ArrayList<FlamePair>();
            flameContext.getKVS().persist("index");
            if (flameContext.getKVS().existsRow("headers", p._1())) {
                String reverse = new String(flameContext.getKVS().get("headers", p._1(), "Context")).split(",title:")[0];
                flameContext.getKVS().put("index", reverse, "urls", p._2());
            }
            return ret;
        });

        flamePairRDD1.saveAsTable("agg");
        kvsClient.delete("agg");
    }

    public static String wordStem(String input) {
        Stemmer s = new Stemmer();
        char[] chars = input.toCharArray();
        s.add(chars, chars.length);
        s.stem();
        return s.toString();
    }

    public static String wordTrim(String input) {
        return input.replaceAll("<.*?>", "").replaceAll("&nbsp;", "\\s").replaceAll("&#160;", "\\s").replaceAll("&quot;","\\s");
    }


}
