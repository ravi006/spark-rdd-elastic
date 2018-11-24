
import CustomerOrbitConstruct.OrbitConstruct
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.reflect.ClassTag

class CustomerOrbitConstruct extends Logging with Serializable {

  def run[Attr: ClassTag](id_master: RDD[String], demoGrap: RDD[String],product_info: RDD[String], networkJson: RDD[String],
                          id_events: RDD[String], id_network_signal: RDD[String]) = {


    val productInfoRDD = product_info.map(b => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(b)
      val id = (parsedJson \ "id").extract[String]
      (id, b)
    }).reduceByKey(_ + _).map { case (n, listProdInfo) =>
      (n, s"[${listProdInfo.replace("}{","},{")}]")
    }

    val masterIdRDD = id_master.map(r => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(r)
      val id = (parsedJson \ "id").extract[String]
      (id, r)
    }).reduceByKey(_ + _).map { case (key, master) =>
      (key, master.replace("}{","},{"))
    }

    val networkRDD = networkJson.map(n => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(n)
      val id = (parsedJson \ "frm_iden_id").extract[String]
      (id, n)
    }).reduceByKey(_ + _).map { case (key, listNetwork) =>
      (key, s"[${listNetwork.replace("}{","},{")}]")
    }


    val networkGraphRDD = networkJson.map(n => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(n)
      val id = (parsedJson \ "frm_id").extract[String]
      val to_id = (parsedJson \ "to_id").extract[String]
      val dos = (parsedJson \ "dos").extract[String]
      (id, n)
    }).reduceByKey(_ + _).map { case (key, listNetwork) =>
      (key, listNetwork.replace("}{","}&{").split("&"))
    }.map(r => {
      val nodes: List[String] = List.empty
      val summaries: mutable.Map[String, List[String]] = mutable.Map.empty
      r._2.foreach { t =>
        implicit val formats = DefaultFormats
        val parsedJson = parse(t)
        val dos: String = (parsedJson \ "dos").extract[String]

        if (dos.isEmpty | dos != null){
          if (summaries.get(dos) != null) summaries.update(dos, t::nodes)
          else summaries.update(dos, t::nodes)
        }
      }
      val all: mutable.Iterable[String] = summaries.map{case (ds, dsJosn) => {
        val tapil: String = dsJosn.mkString(",")
        s"'$ds' : [$tapil]"
      }
      }
      (r._1, all.mkString(","))
    }).map(y => (y._1, s"{${y._2}}"))


    val eventsRDD = id_events.map(e => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(e)
      val id = (parsedJson \ "id").extract[String]
      (id, e)
    }).reduceByKey(_ + _).map { case (e, listEvents) =>
      (e, s"[${listEvents.replace("}{","},{")}]")
    }

    val networkSignalRDD = id_network_signal.map(s => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(s)
      val id = (parsedJson \ "id").extract[String]
      (id, s)
    }).reduceByKey(_ + _).map { case (s, listNetworkSignal) =>
      (s, s"[${listNetworkSignal.replace("}{","},{")}]")
    }


    val masterWithRDD = id_master.map(r => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(r)
      val id = (parsedJson \ "index").extract[String]
      (id, r)
    }).reduceByKey(_ + _).map { case (n, listProdInfo) =>
      (n, s"${listProdInfo.replace("}{","},{")}")
    }

    val demographicJoin = masterWithRDD.leftOuterJoin(demoGrap.map(r => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(r)
      val id = (parsedJson \ "index").extract[String]
      (id, r)
    }).map(x => (x._1, x._2)))
      .map { case (key, (v1, v2: Option[String])) => (key, (v1, v2.getOrElse("{}"))) }
    val resultRDD = demographicJoin.map{
      case (id, list) =>
        val parsedJson = parse(list._1)
        val key = (parsedJson \ "index").extract[String]
        (key, s"'personal_info' : ${list._1},'demographic' : ${list._2}")
    }.reduceByKey(_ + _).map { case (key, master) =>
      (key, master.replace("}{","},{"))
    }.leftOuterJoin(productInfoRDD.map(x => (x._1, x._2)))
      .map { case (key, (v1, v2: Option[String])) => (key, (v1, v2.getOrElse("[]"))) }
      .leftOuterJoin(networkGraphRDD)
      .map { case (k, (v, w: Option[Any])) => (k, (v, w.getOrElse("{}"))) }
      .leftOuterJoin(eventsRDD)
      .map { case (a, (b, c: Option[String])) => (a, (b, c.getOrElse("[]"))) }
      .leftOuterJoin(networkSignalRDD).map { case (a, (b, c: Option[String])) => (a, (b, c.getOrElse("[]"))) }
      .sortBy(_._1)
      .map {
        case (id, jsonString) =>
          new OrbitConstruct(id.toLong, jsonString._1._1._1._1, jsonString._2, jsonString._1._1._1._2, jsonString._1._1._2, jsonString._1._2)
      }

    resultRDD
  }

object CustomerOrbitConstruct{

  class OrbitConstruct[Attr: ClassTag](id: BigInt, j1: String, j2: Attr, j3: Attr, j4: Attr, j5: Attr) extends Serializable {

    def customerOrbit: String = s"{'id' : '$id','$id' : {[$j1],'product_info': $j2,'events': $j3,'network_properties': $j4,'network_signal': $j5}}".replace("'", "\"").stripMargin

  }

}
