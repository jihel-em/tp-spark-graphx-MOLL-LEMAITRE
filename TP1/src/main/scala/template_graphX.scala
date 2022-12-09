import scala.Console.println
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object template extends App {
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)

  val hF = new HelpfulFunctions

  val DATASET_PATH = "src/main/resources/"
  /*  Graph creation  */
  val users = sc.textFile(DATASET_PATH +"users.txt")
  val followers = sc.textFile(DATASET_PATH +"followers.txt")

  //Création RDD
  var rdd = sc.textFile(DATASET_PATH + "JC-202112-citibike-tripdata.csv").map(row => row.split(','))
  val headerRow = rdd.first()

  val col_station_start_id = headerRow.indexOf("start_station_id")
  val col_station_start_name = headerRow.indexOf("start_station_name")
  val col_station_start_lat = headerRow.indexOf("start_lat")
  val col_station_start_lng = headerRow.indexOf("start_lng")

  val col_station_end_id = headerRow.indexOf("end_station_id")
  val col_station_end_name = headerRow.indexOf("end_station_name")
  val col_station_end_lat = headerRow.indexOf("end_lat")
  val col_station_end_lng = headerRow.indexOf("end_lng")

  val col_id_trip = headerRow.indexOf("ride_id")
  val col_rideable = headerRow.indexOf("rideable_type")
  val col_start_date = headerRow.indexOf("started_at")
  val col_end_date = headerRow.indexOf("ended_at")
  val col_member_casual = headerRow.indexOf("member_casual")

  rdd = rdd.filter(row => {
    var toKeep = true
    row.foreach(str => {
      if (str.compareTo("") != 0){
        toKeep = false
      }
    })
    toKeep
  }).filter(row => row(0) != headerRow(0))

  val rdd_stations_start = rdd.map(row =>
    new Station(
      row(col_station_start_id),
      row(col_station_start_name),
      row(col_station_start_lat).toFloat,
      row(col_station_start_lng).toFloat
  )).distinct()


  println("\r\r\r\r\r\rHAAAAAAAAAAAAAAAAAAAAAAAAAA\r\r\r\r\r\r")
  val rdd_stations_end = rdd.map(row => {
    println("\r\r\r\r\r\rHEEEEEEHOOOOOOOOOOOOOOOO\r\r\r\r\r\r")
    new Station(
      row(col_station_end_id),
      row(col_station_end_name),
      row(col_station_end_lat).toFloat,
      row(col_station_end_lng).toFloat
    )
  } ).distinct()

  var currentId = 0
  var mapId = Map[String, Int]()

  val rdd_stations = rdd_stations_start.union(rdd_stations_end).distinct().map(station => {
    val newId = currentId
    currentId = currentId + 1
    mapId += (station.getId -> newId)
    (newId.toLong, station)
  })



  val rdd_trips: RDD[Edge[Trip]] = rdd map { row =>
    val start_station = row(col_station_start_id)
    val end_station = row(col_station_end_id)
    val id = row(col_id_trip)
    val rideable = row(col_rideable)
    val startDate = row(col_start_date)
    val endDate = row(col_end_date)
    val member_casual = row(col_member_casual)
    (Edge(mapId.get(start_station).get, mapId.get(end_station).get, new Trip(id, rideable, hF.timeToLong(startDate), hF.timeToLong(endDate), start_station, end_station, member_casual)))
  }

  val graph = Graph(rdd_stations, rdd_trips)

  val subgraph = graph.subgraph(et => et.attr.startDate>=hF.timeToLong("2021-12-05 00:00:00") && et.attr.endDate<hF.timeToLong("2021-12-26 00:00:00"))

  val outDegreeStations = graph.aggregateMessages[Int](_.sendToSrc(1), _ + _)
  outDegreeStations.collect().foreach(println)














  /*  Création de graphe */
  /*

  Les étapes suivantes consistent à convertir L'RDD users en VertexRDD[VD] et l'RDD followers en EdgeRDD[ED, VD],
  puis à créer un graphe avec ces deux RDDs.

   */

  case class User(name: String, nickname: String)

  //Convertissez chaque élément de l'RDD users en objet de classe user.

  val usersRDD: RDD[(VertexId, User)] = users.map{line =>
    val row = line split ','
    if (row.length > 2){
      (row(0).toLong, User(row(1), row(2)))
    }else {
      (row(0).toLong, User(row(1), "NOT DEFINED"))
    }
  }

  //Transformez chaque élément de l 'RDD followers en Edge[Int]

  val followersRDD: RDD[Edge[Int]] = followers map {line =>
    val row = line split ' '
    (Edge(row(0).toInt, row(1).toInt, 1))
  }

  //Contruisez le graphe à partir de usersRDD et followersRDD

  val toyGraph : Graph[User, Int] =
    Graph(usersRDD, followersRDD)

  toyGraph.vertices.collect()
  toyGraph.edges.collect()

  /*  Extraction de sous-graphe  */

  /*

  Affichez l'entourage des noeuds Lady gaga et Barack Obama avec une solution qui fait appel
  à la fonction \textit{subgraph}.

   */

  val desiredUsers = List("ladygaga", "BarackObama")

  toyGraph.subgraph(edgeTriplet => desiredUsers contains edgeTriplet.srcAttr.name )
    .triplets.foreach(t => println(t.srcAttr.name + " is followed by " + t.dstAttr.name))

  /*  Computation of the vertices' Degrees  */

  /*

  Calculez le degré (Nombre de relations sortantes) de chaque noeuds en faisant appel
  à la méthode \texttt{aggregateMessage}.

   */

  val outDegreeVertexRDD = toyGraph.aggregateMessages[Int](_.sendToSrc(1), _+_)
  outDegreeVertexRDD.collect().foreach(println)

  /* Calcul de la distance du plus long chemin sortant de chaque noeud. */

  /*

  Créez une nouvelle classe \texttt{UserWithDistance} pour inclure la distance aux noeuds du graphe.

  */

  case class UserWithDistance(name: String, nickname: String, distance: Int)

  /*
  * Transformez les VD des noeuds du graphe en type UserWithDistance et initialisez la distance à 0.

  * Définissez la méthode vprog (VertexId, VD, A) => VD. Cette fonction met à jour les données
    VD du noeud ayant un identifiant VD en prenant en compte le message A

  * Définissez la méthode sendMsg EdgeTriplet[VD, ED] => 	Iterator[(VertexId, A)].
    Ayant les données des noeuds obtenues par la méthode vprog VD et des relations ED.
    Cette fonction traite ces données pour créer un message $A$ et l'envoyer ensuite au noeuds destinataires.

  * Définissez la méthode mergeMsg (A, A) => A. Cette méthode est identique à la méthode reduce
    qui traite tous les messages reçus par un noeud et les réduit en une seule valeur.

  *

   */

  val toyGraphWithDistance = Pregel(toyGraph.mapVertices((_, vd) => UserWithDistance(vd.name, vd.nickname, 0)),
        initialMsg=0, maxIterations=2,
        activeDirection = EdgeDirection.Out) (
        (_:VertexId, vd:UserWithDistance, a:Int) => UserWithDistance(vd.name, vd.nickname, math.max(a, vd.distance)),
        (et:EdgeTriplet[UserWithDistance, Int]) => Iterator((et.dstId, et.srcAttr.distance+1)),
        (a:Int, b:Int) => math.max(a,b))
  toyGraphWithDistance.vertices.foreach(user =>println(user))
}
