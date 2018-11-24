
import connectors.DBConnectors
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

class CustomerOrbit(sparkSession: SparkSession, conf: Config) {

  import org.apache.spark.sql.functions._
  import sparkSession.implicits._



  def createCustOrbit() = {

    val product_info: RDD[String] = DBConnectors.readDataFromMysql(sparkSession, "product_info", conf).toJSON.rdd

    val id_master: RDD[String] = sparkSession.sqlContext.sql("select * from database.id_dev").toJSON.rdd

    val demograph = sparkSession.sqlContext.sql("select * from database.cust_demographics WHERE index IS NOT NULL").toJSON.rdd

    val network = DBConnectors.readDataFromMysql(sparkSession, "network", conf)

    val ntwrk_path = DBConnectors.readDataFromMysql(sparkSession, "network_path", conf)

    val id_events: RDD[String] = DBConnectors.readDataFromMysql(sparkSession, "id_events", conf).toJSON.rdd

    val id_network_signal: RDD[String] = DBConnectors.readDataFromMysql(sparkSession, "id_network_signal", conf).toJSON.rdd

    val networkJson: RDD[String] = ntwrk_path.join(network, network("ntwrk_type_id") === ntwrk_path("ntwrk_type_id"), "inner")
      .drop(network("ntwrk_type_id")).drop(network("ntwrk_id")).toJSON.rdd


    val customerOrbitConstruct = new CustomerOrbitConstruct()

    customerOrbitConstruct.run(id_master, demograph, product_info, networkJson, id_events, id_network_signal)
  }

  def updateESIndex() = {

    createCustOrbit().saveJsonToEs("test_cust_orbit/docs", scala.collection.Map(ConfigurationOptions.ES_MAPPING_ID -> "id"))

  }

}
