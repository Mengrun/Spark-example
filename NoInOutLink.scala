//import org.apache.log4j.{Level, Logger}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        //val links_file = "links-simple-sorted.txt"
        //val titles_file = "titles-sorted.txt"
        val num_partitions = 10
        

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val links = sc
            .textFile(links_file, num_partitions)
   
        val frompage = links.map(line => line.split("\\:")(0)).map(r => (r,1))
        val topage = links.map(line => line.split("\\:")(1))
                    .flatMap(line => line.split(" "))
                    .map(l=>(l,1))
                    .reduceByKey(_+_)
                    .map{case (u,v) => (u.toString,v.toString)}


        frompage.collect().foreach(println)
        //topage.collect().foreach(println)

        val titles = sc
            .textFile(titles_file, num_partitions)
        val pair = titles.zipWithIndex
        val indexKey = pair.map{case (k,v) => (v,k)}
                        .map{x => ((x._1+1).toString,x._2)}

        val outlink = frompage.join(indexKey)
                        .map{case (k,(u,v)) => (k.toString,v)}
        println("***")
        frompage.join(indexKey).take(5).foreach(println)

        val inlink = topage.join(indexKey)
                        .map{case (k,(u,v)) => (k.toString,v)}



        /* No Outlinks */
        //val outlink_bc = outlink.collect.toSet
        //val no_outlinks = indexKey.filter{case (u, v) => !outlink_bc.contains(u,v)}
        val no_outlinks = indexKey.subtractByKey(outlink)
        println("[ NO OUTLINKS ]")
        //no_outlinks.take(10).foreach(println)
        no_outlinks.takeOrdered(10)(Ordering[Int].on(x => x._1.toInt)).foreach(println)

        /* No Inlinks */
        //val inlink_bc = inlink.collect.toSet
        //val no_inlinks = indexKey.filter{case(u,v) => !inlink_bc.contains(u,v)}
        val no_inlinks = indexKey.subtractByKey(inlink)
        println("\n[ NO INLINKS ]")
        //no_inlinks.take(10).foreach(println)
        no_inlinks.takeOrdered(10)(Ordering[Int].on(x => x._1.toInt)).foreach(println)
    }
}