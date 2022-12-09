import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object CityBike extends App {
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)
  val helpFunctions = new HelpfulFunctions();

  val DATA_PATH = "JC-202112-citibike-tripdata.csv"

  val caract_rdd = sc.textFile(DATA_PATH).map(row => row.split(','))
  val headerRow = caract_rdd.first()
  val rdd = caract_rdd.filter(row => row(0) != headerRow(0))

  /*  Création de graphe */

  case class Station(id: String, name: String, lat: Float, lgn: Float)

  val col_station_start_id = headerRow.indexOf("start_station_id")
  val col_station_start_name = headerRow.indexOf("start_station_name")
  val col_station_start_lat = headerRow.indexOf("start_lat")
  val col_station_start_lng = headerRow.indexOf("start_lng")

  val rdd_stations_start = rdd
    .map(row => Station(
      row(col_station_start_id),
      row(col_station_start_name),
      row(col_station_start_lat).toFloat,
      row(col_station_start_lng).toFloat)
  ).distinct

  val col_station_end_id = headerRow.indexOf("end_station_id")
  val col_station_end_name = headerRow.indexOf("end_station_name")
  val col_station_end_lat = headerRow.indexOf("end_lat")
  val col_station_end_lng = headerRow.indexOf("end_lng")

  val rdd_stations_end = rdd.filter(row => row(col_station_end_lat).compareTo("") != 0 && row(col_station_end_lng).compareTo("") != 0)
    .map(row => Station(
    row(col_station_end_id),
    row(col_station_end_name),
    row(col_station_end_lat).toFloat,
    row(col_station_end_lng).toFloat
  )).distinct()

  var currentId = 0
  var mapId:Map[String,Int] = Map()


  val all_stations = rdd_stations_start.union(rdd_stations_end).distinct();

  all_stations.foreach(station => {
    currentId = currentId + 1
    mapId += (station.id -> currentId)
  });

  val rdd_stations: RDD[(VertexId, Station)] = all_stations.map(station => (mapId.get(station.id).get, station))

  //Transformez chaque élément de l 'RDD followers en Edge[Int]

  case class Trip(id: String, bikeType: String, started_at: Long, ended_at: Long, start_station_id: String, end_station_id: String, member_casual: String)

  val tripsRDD: RDD[Edge[Trip]] = rdd.filter(row => row(col_station_start_id).compareTo("") != 0 && row(col_station_end_id).compareTo("") != 0)
    .map { row =>
    (Edge(mapId.get(row(col_station_start_id)).get, mapId.get(row(col_station_end_id)).get, (Trip(row(headerRow.indexOf("ride_id")), row(headerRow.indexOf("rideable_type")), helpFunctions.timeToLong(row(headerRow.indexOf("started_at"))), helpFunctions.timeToLong(row(headerRow.indexOf("ended_at"))), row(headerRow.indexOf("start_station_id")), row(headerRow.indexOf("end_station_id")), row(headerRow.indexOf("member_casual"))))))
  }

  //Contruisez le graphe

  val tripGraph : Graph[Station, Trip] =
    Graph(rdd_stations, tripsRDD)

  tripGraph.vertices.collect()
  tripGraph.edges.collect()

  /*  Extraction de sous-graphe  */

  val subGraph = tripGraph.subgraph(edge => edge.attr.started_at >= helpFunctions.timeToLong("2021-12-05") && edge.attr.ended_at <= helpFunctions.timeToLong("2021-12-25"))

  val outDegreeVertexRDD = tripGraph.aggregateMessages[Int](_.sendToSrc(1), _+_)
  val inDegreeVertexRDD = tripGraph.aggregateMessages[Int](_.sendToDst(1), _+_)

  println("Station ayant le plus de trajets sortants : ")
  outDegreeVertexRDD.sortBy(station => station._2, ascending = false).take(10).foreach(station => println(rdd_stations.filter(f => f._1 == station._1).first()._2.name + " : " + station._2))

  println("Station ayant le plus de trajets entrants : ")
  inDegreeVertexRDD.sortBy(station => station._2, ascending = false).take(10).foreach(station => println(rdd_stations.filter(f => f._1 == station._1).first()._2.name + " : " + station._2))


  case class StationWithDistance(id: String, name: String, lat: Float, lgn: Float, distance: Double)

  val tripGraphWithDistance = Pregel(tripGraph.mapVertices((_, sd) => StationWithDistance(sd.id, sd.name, sd.lat, sd.lgn, 0)),
    initialMsg = 0, maxIterations = 2,
    activeDirection = EdgeDirection.Out)(
    (_: VertexId, sd: StationWithDistance, a: Int) => StationWithDistance(sd.id, sd.name, sd.lat, sd.lgn, math.max(a, sd.distance)),
    (et: EdgeTriplet[StationWithDistance, Trip]) => Iterator((et.dstId, (et.srcAttr.distance + helpFunctions.getDistKilometers(et.dstAttr.lgn, et.dstAttr.lat, et.srcAttr.lgn, et.srcAttr.lat)).toInt)),
    (a: Int, b: Int) => math.max(a, b))
  tripGraphWithDistance.vertices.sortBy(station => station._2.distance, ascending = false).foreach(station => println(station))
  println("Station la plus proche de la station JC013 en distance : " + tripGraphWithDistance.vertices.sortBy(station => station._2.distance, ascending = true).first())


  val tripGraphWithDistanceBis = Pregel(tripGraph.mapVertices((_, sd) => StationWithDistance(sd.id, sd.name, sd.lat, sd.lgn, 0)),
    initialMsg = 0, maxIterations = 2,
    activeDirection = EdgeDirection.Out)(
    (_: VertexId, sd: StationWithDistance, a: Int) => StationWithDistance(sd.id, sd.name, sd.lat, sd.lgn, math.max(a, sd.distance)),
    (et: EdgeTriplet[StationWithDistance, Trip]) => Iterator((et.dstId, ((et.srcAttr.distance + et.attr.ended_at - et.attr.started_at).toInt))),
    (a: Int, b: Int) => math.max(a, b))
  tripGraphWithDistanceBis.vertices.sortBy(station => station._2.distance, ascending = false).foreach(station => println(station))
  println("Station la plus proche de la station JC013 en temps : " + tripGraphWithDistanceBis.vertices.sortBy(station => station._2.distance, ascending = true).first())
}
