import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
//airlines

val vertices=Array((1L, ("SFO")),(2L, ("ORD")),(3L,("DFW")),(4L,("BRW")),(5L,("JFD")))
val vRDD= sc.parallelize(vertices)
val edges = Array(Edge(1L,2L,1800),Edge(2L,3L,800),Edge(3L,4L,1400)),Edge(4L,5L,400)),Edge(5L,1L,1400))
val eRDD= sc.parallelize(edges)
// Array(Edge(1,2,1800), Edge(2,3,800))
val nowhere = ("nowhere")
val graph = Graph(vRDD,eRDD, nowhere)

graph.vertices.collect.foreach(println)


graph.edges.collect.foreach(println)


graph.triplets.collect.foreach(println)


println(graph.inDegrees)
//VertexRDDImpl[83] at RDD at VertexRDD.scala:57

val numbusstation = graph.numVertices
//numbusstation: Long = 3

val numroutes = graph.numEdges
//numroutes: Long = 3






val ranks = graph.pageRank(0.1).vertices

ranks.take(3)


val impbusstations = ranks.join(vRDD).sortBy(_._2._1, false).map(_._2._2)
impbusstations.collect.foreach(println)

val connectedComponents = graph.connectedComponents().vertices
println("GCC:")
connectedComponents.foreachpartition(println)

val trCounting=graph.triangleCount().vertices
println("Triangle counting:")
trCounting.foreach(println)






