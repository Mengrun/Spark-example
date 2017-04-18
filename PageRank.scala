import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object PageRank {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)


        val links = sc
            .textFile(links_file, num_partitions)


        val frompage = links.map(line => (line.split(":")(0),line.split(":")(1)))


        val topage = links.map(line => line.split("\\:")(1))
                        .flatMap(line => line.split(" "))
                        .map(l=>(l,1))
                        .reduceByKey(_+_)
                        .map{case (u,v) => (u.toString,v.toString)}


        val titles = sc
            .textFile(titles_file, num_partitions)

        val N = titles.count
        val pair = titles.zipWithIndex
        val indexKey = pair.map{case (k,v) => (v,k)}
                        .map{x => ((x._1+1).toString,x._2)}

        val inlink = topage.join(indexKey)
                    .map{case (k,(u,v)) => (k.toString,v)}
        //println(titles.count)

        val no_inlinks = indexKey.subtractByKey(inlink)
                        .map{case (u,v) => (u,0.15*100/N)}

        var rank = frompage
                    .mapValues(r => 100/N.toDouble)




        /* PageRank */
        for (i <- 1 to iters) {
            val contribution = frompage.join(rank).values.flatMap{ case (u,v) =>
                                val size = u.split("\\s+").filter(r=> r!="").size
                                u.split("\\s+").filter(r=> r!="")
                                                .map(u => (u.toString, v/size))
                                }

            rank = contribution.reduceByKey(_ + _).mapValues(0.15*100/N + 0.85 * _)
                    .union(no_inlinks)


        }
        val total = rank.values.sum
        val finalrank = rank.mapValues(v => v*100/total)
                        .join(indexKey)
                        .map{case(a,(b,c)) => (a,c,b)}
        println("[ PageRanks ]")

        //finalrank.sortBy(_._3,false).collect.foreach(println)
        finalrank.takeOrdered(10)(Ordering[Double].reverse.on(x => x._3)).foreach(println)

    }
}
