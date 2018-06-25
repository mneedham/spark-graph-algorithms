import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object SimpleApp {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val v = sqlContext.createDataFrame(List(
      ("home", "Home"),
      ("about", "About"),
      ("product", "Product"),
      ("links", "Links"),
      ("a", "Site A"),
      ("b", "Site B"),
      ("c", "Site C"),
      ("d", "Site D")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("home", "about", "LINKS"),
      ("about", "home", "LINKS"),
      ("product", "home", "LINKS"),
      ("home", "product", "LINKS"),
      ("links", "home", "LINKS"),
      ("home", "links", "LINKS"),
      ("links", "a", "LINKS"),
      ("a", "home", "LINKS"),
      ("links", "b", "LINKS"),
      ("b", "home", "LINKS"),
      ("links", "c", "LINKS"),
      ("c", "home", "LINKS"),
      ("links", "d", "LINKS"),
      ("d", "home", "LINKS")
    )).toDF("src", "dst", "relationship")


    val g = GraphFrame(v, e)

    // Query: Get in-degree of each vertex.
    g.inDegrees.show()

    // Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    // Run PageRank algorithm, and show results.
    val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
    results.vertices.select("id", "pagerank").sort(desc("pagerank")).show()
  }
}
