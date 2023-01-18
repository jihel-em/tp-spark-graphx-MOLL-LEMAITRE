import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.apache.spark.{SparkConf, SparkContext}
import tp.headerRow

import scala.Console.println

object tp extends App{
  def getScore(rdd: RDD[Array[String]]): RDD[(String, Float, Int)] = {
    val headerRow = rdd.first()
    val col_player_names = headerRow.indexOf("player_name")
    val col_assist = headerRow.indexOf("player_assists")
    val col_damage = headerRow.indexOf("player_dmg")
    val col_kills = headerRow.indexOf("player_kills")
    val col_position = headerRow.indexOf("team_placement")

    val res = rdd.filter(row => row(0) != headerRow(0))
      .map(row => (row(col_player_names),
        50 * row(col_assist).toFloat + row(col_damage).toFloat + 100 * row(col_kills).toFloat + (100 - row(col_position).toFloat) * 10))
      .groupByKey()
      .map(row => (
        row._1, row._2.map(strScore => strScore.toFloat).sum / row._2.size, row._2.size))

    return res
  }

  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)

  val MATCH_DATA_PATH = "src/main/resources/agg_match_stats_0.csv" //download from https://www.kaggle.com/ahmedlahlou/accidents-in-france-from-2005-to-2016
  val USERS_DATA_FILE = "PATH_TO_USERS_CSV_FILE"

  //On choisit MEMORY_ONLY car on a suffisament de RAM pour. Autrement, sur un ordinateur plus modeste, il serait plus judicieux
  //de choisir MEMORY_AND_DISK.
  val caract_rdd = sc.textFile(MATCH_DATA_PATH).map(row => row.split(',')).persist(MEMORY_ONLY)
  val headerRow = caract_rdd.first()
  val caract_rdd_clean = caract_rdd.filter(row => row(0) != headerRow(0))


  val col_names = headerRow.indexOf("player_name")
  val col_kills = headerRow.indexOf("player_kills")
  val col_position = headerRow.indexOf("team_placement")
  val names_pos = caract_rdd_clean.map(row => (row(col_names), row(col_position)))
  //println(names_kills.collect().mkString("\n"))

  val mean_nb_pos = names_pos.groupByKey().map(row=>(row._1,row._2.map(str=>str.toFloat).sum/row._2.size,row._2.size))
  //println(mean_nb_kills.collect().mkString("\n"))
  //println(mean_nb_pos.filter(row=>row._3>=4).sortBy(row=>row._2,ascending = true).take(10).mkString("\n"))

  val rdd_score = getScore(caract_rdd)
  println("nombre de joueurs distincts : "+rdd_score.count())
  println(rdd_score.filter(row=>row._3>=4).sortBy(row=>row._2,ascending = false).take(10).mkString("\n"))

}

