package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.webserver.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cis5550.jobs.Crawler.*;
import static java.lang.Math.*;

public class PageRank {
    private static final Logger logger = Logger.getLogger(Server.class);
    static Hasher hasher = new Hasher();
    private static final AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) {
        logger.info("main seed url ");
    }

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        KVSClient kvsClient = flameContext.getKVS();
        double threhold = Double.parseDouble(args[0]);


        FlameRDD stable = flameContext.fromTable("crawl", r -> {
            if (r.get("url") == null || r.get("page") == null) return null;
            ArrayList<String> arrays = extractURL(r);
            ArrayList<String> results = new ArrayList<>();
            String i;
            String main_url = URLcheck(r.get("url"),r.get("url"));

            try {
                flameContext.getKVS().persist("id_url");
//                flameContext.getKVS().persist("url_id");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


                for (String entry : arrays) {
                    if (entry == null) continue;
                if (entry.contains("redirect_uri=")) {

                } else if (entry.contains("..")) {
                    continue;
                } else if (entry.endsWith(".jpg") || entry.endsWith(".jpeg") || entry.endsWith(".gif") || entry.endsWith(".png") || entry.endsWith(".txt")) {
                    continue;
                } else if (entry.length() > 100) {
                    continue;
                } else if (entry.contains(",")) {
                    continue;
                } else {

                    String rowName = hasher.hash(entry);
                    logger.info("count: "+ rowName);
                    try {
                        if (!flameContext.getKVS().existsRow("id_url", rowName)) {
                            flameContext.getKVS().put("id_url", rowName, "url", entry);
//                            flameContext.getKVS().put("url_id", entry, "id", rowName);
                        }
                        byte[] row = flameContext.getKVS().get("url_id",entry,"id");
                        if (row != null)
                        results.add(new String(row));
                    } catch (Exception e) {
                        continue;
                    }

                }

                }
            String mainHash = hasher.hash(r.get("url"));
            try {
                if (!flameContext.getKVS().existsRow("id_url", mainHash)) {
                    flameContext.getKVS().put("id_url", mainHash, "url", r.get("url"));
                }
                byte[] row = flameContext.getKVS().get("url_id",r.get("url"),"id");
                if (row != null)
                main_url = new String(row);
            } catch (Exception e) {
                return null;
            }
//                logger.info("stable: " + main_url + ";" + "1.0,1.0,urls:" + results );
            return main_url + "val;" + "1.0,1.0,urls:" + results;
//            }
//            return null;
        });

//

        FlamePairRDD map = stable.mapToPair(p -> {
            if (p == null) return null;
            return new FlamePair(p.split("val;")[0], p.split("val;")[1]);
        });
        stable.saveAsTable("stable");
        kvsClient.delete("stable");


        int i = 0;
        while (true) {
            i++;
            if (i > 2) {
                kvsClient.delete("joined_" + (i - 2));
                kvsClient.delete("diffs_" + (i - 2));
                kvsClient.delete("agg_" + (i - 2));
            }
            logger.info("iteration: " + i);
            FlamePairRDD addRank = map.flatMapToPair(p -> {
                if (p == null) return null;
                Boolean selfLink = false;
//                logger.info("before addrank: " + p._2());
//                logger.info("p:" + p._1()+" "+p._2());
                double current = Double.parseDouble(p._2().split(",")[0]);
                ArrayList<FlamePair> ranks = new ArrayList<>();

                if (p._2().split("urls:").length >= 2) {

                    String[] contains = p._2().split("urls:")[1].split(", ");

                    for (String url : contains) {
                        url = url.replaceAll("[\\[\\]]", "");

                        double newRank = 0.85 * current / contains.length;
                        if (!url.equals("")) {

                            ranks.add(new FlamePair(url, String.valueOf(newRank)));
                        }
                        if (url.equals(p._1())) {
                            selfLink = true;
                        }

                    }

                }
                if (!selfLink) ranks.add(new FlamePair(p._1().replaceAll("[\\[\\]]", ""), "0.0"));
//                logger.info("COMPLETE RANKING");
                return ranks;
            });
//            logger.info("addRank:" + addRank.collect().toString() +"\n");

            FlamePairRDD agg = addRank.foldByKey("0", (String a, String b) -> {

                if (!a.equals("")) {
                    return ("" + (Double.parseDouble(a) + Double.parseDouble(b))).replaceAll("[\\[\\]]", "");
                }
                return b;
            });

            addRank.saveAsTable("addRank_" + i);
            agg.saveAsTable("agg_" + i);
            kvsClient.delete("addRank_" + i);

            FlamePairRDD joined = map.join(agg).flatMapToPair(p -> {
//                        logger.info("join p: "+p.toString());
                        String[] splits = p._2().split(",");
                        double current = Double.parseDouble(splits[0]);
                        ArrayList<FlamePair> ranks = new ArrayList<>();
                        int start = p._2().indexOf("[") + 1;
                        int end = p._2().indexOf("]");
                        String contains;
                        if (start > 0 && end > 0 && end > start) {
                            contains = p._2().substring(start, end);
                            double newRank = Double.parseDouble(splits[splits.length - 1]);
                            ranks.add(new FlamePair(p._1(), (newRank + 0.15) + "," + current + ",urls:" + "[" + contains + "]"));
                        } else {
                            ranks.add(new FlamePair(p._1(), p._2().substring(0, p._2().lastIndexOf(','))));
                        }
                        return ranks;
                    }
            );

            map = joined;

            joined.saveAsTable("joined_" + i);

            FlameRDD diffs = map.flatMap(p -> {
                String[] splits = p._2().split(",");
                double current = Double.parseDouble(splits[0]);
                double previous = Double.parseDouble(splits[1]);
                return Arrays.asList(String.valueOf(abs(current - previous)));
            });

            String max = diffs.fold("0", (s1, s2) -> String.valueOf(max(Double.parseDouble(s1), Double.parseDouble(s2))));
            logger.info("max: " + max);
            if (args.length == 2) {
                double rate1 = Double.valueOf(args[0]);
                double ratio_input = Double.valueOf(args[1]) / 100.0;
                int count = diffs.count();
                if (count ==0 ) break;
                FlameRDD criterion = diffs.flatMap(m -> {
                    List<String> ret = new ArrayList<String>();
                    double value1 = Double.valueOf(m);
                    if (rate1 > value1) ret.add(m);
                    return ret;
                });

                Double ratio = Double.valueOf(criterion.count()) / Double.valueOf(count);

//	System.out.println("ratio_total: "+ count+ " ratio_part: "+ criterion.count()+ " and the ratio is: " + ratio);
                if (ratio >= ratio_input) {
                    break;
                }

            }
            diffs.saveAsTable("diffs_" + i);

        }
//        map.saveAsTable("map");
//        kvsClient.delete("map");
        kvsClient.delete("pageranks");

        FlamePairRDD result = map.flatMapToPair(p -> {
            List<FlamePair> ret = new ArrayList<FlamePair>();
            String[] splits = p._2().split(",");
            String current = splits[0];
            String url = new String(flameContext.getKVS().get("id_url", p._1(), "url"));
//            logger.info("result " + url);
            flameContext.getKVS().persist("pageranks");
            flameContext.getKVS().put("pageranks", url, "rank", current);
            ret.add(new FlamePair(p._1(), current));
            return ret;
        });


//        map.saveAsTable("map");
//        kvsClient.delete("map");




    }

    public static String URLcheck(String url, String urlbase) {
        String output = null;
        int p = url.indexOf("#");

        if (p != -1 && p == 0) {
            return null;
        } else if (p != -1 && p != 0) {
            url = url.substring(0, url.indexOf("#"));
        }

        if (url.startsWith("https://")) {
            int temp = url.indexOf("/", url.indexOf("/") + 2);
            int temp2 = url.indexOf(":", url.indexOf(":") + 1);
            if ((temp2 > temp || temp2 == -1) && temp > 0) {
                url = url.substring(0, temp) + ":443" + url.substring(temp);
            }
            output = url;
        } else if (url.startsWith("http://")) {
            int temp = url.indexOf("/", url.indexOf("/") + 2);
            int temp2 = url.indexOf(":", url.indexOf(":") + 1);
            if (temp == -1) {
                url = url + "/";
                temp = url.length() - 1;
            }
            if (temp2 == -1 && temp >= 0) {
                url = url.substring(0, temp) + ":80" + url.substring(temp);
            }
            output = url;
        } else if (url.startsWith("../")) {
            int temp =urlbase.indexOf("/", urlbase.indexOf("/") + 2);
            if (temp != -1)  urlbase = urlbase.substring(0, temp);
            output = urlbase + url.substring(2);
        } else if (url.startsWith("/")) {
            int temp = urlbase.indexOf("/", urlbase.indexOf("/") + 2);
            if (temp != -1) {
                urlbase = urlbase.substring(0, temp);
                output = urlbase + url;
            }
        } else {
            int temp = urlbase.lastIndexOf("/") + 1;
            if (temp != -1) {
                String cutURL = urlbase.substring(0, temp);
                output = cutURL + url;
            }
        }
        return output;
    }
}