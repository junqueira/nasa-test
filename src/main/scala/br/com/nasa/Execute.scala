package br.com.nasa

import br.com.nasa.utils.SparkUtils
import org.apache.log4j._
import br.com.nasa.utils.Util
import br.com.nasa.access.Access


object Execute {

  val log: Logger = Logger.getLogger(Execute.getClass)

  def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("hive").setLevel(Level.OFF)

      log.info(s"Initialize proccess")

      try {
          args.grouped(2).map(x => x(0).replace("--","") -> x(1)).toMap
      } catch {
          case _ : Exception => showHelp()
      }

      val params = args.grouped(2).map(x => x(0).replace("--","") -> x(1)).toMap
      val config = params.getOrElse("config", "")
      val fonte = params.getOrElse("fonte", "").toLowerCase
      val file_name = params.getOrElse("file", "")

      if (fonte.isEmpty || args.contains("-h"))
          showHelp()

      val spark = SparkUtils.getSparkSession(fonte)

      log.info(s"**********************************************************************************")
      log.info(s"*** Fonte: $fonte")
      log.info(s"**********************************************************************************")

      try {
          fonte match {

              case "nasa_access" =>
                  log.info(s"​Requests​ ​to​ ​the​ ​NASA​ ​Kennedy​ ​Space​ ​Center​")
                  val config_file = params.getOrElse("config_file", "")
                  Access.execute(spark, config, file_name)

              case _ =>
                  log.error(s"Source not found!")
                  sys.exit(134)
          }
      }catch {
          case e : Exception =>
              log.error(s"Error in proccess: "+e.getMessage)
              e.printStackTrace()
      }

      log.info(s"End proccess nasa")

  }

  def showHelp(): Unit = {
      log.info(s" ==> HELP")
      log.info(s"======================================================================================== ")
      log.info("usage    : spark-submit nasa-test-1.0.jar --fonte $fonte --dtfoto $dtfoto")
      log.info("$fonte   : font name, like (nasa, etc...)")
      log.info("$dtfoto  : dtfoto - yyyyMMdd")
      log.info(s"========================================================================================")
      sys.exit(134)
  }

}
