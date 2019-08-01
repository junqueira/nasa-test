package br.com.nasa.access
import br.com.nasa.Execute.log
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, lit, count, sum}


object Access extends BootTask {
    val log: Logger = Logger.getLogger(Access.getClass)

    def execute(spark: SparkSession, config: String, file_name: String): Unit = {
        try {
            readProperties(config)
            val hdfs_nasa = fs_name + fs_nasa + file_name.split('/').last
            val nasa_df = getDataFrame(spark, hdfs_nasa)
            nasa_df.show(10,false)
            //    val access_df = nasa_df.transform(with_host_request())
            //                       .transform(with_time_stamp())
            //                       .transform(with_request())
            //                       .transform(with_codigo_http())
            //                       .transform(with_bytes_return())
            host_unique(spark, nasa_df)
            total_404(spark, nasa_df)
            top_5_404(spark, nasa_df)
            quantity_404_day(spark, nasa_df)
            total_bytes(spark, nasa_df)
        } catch {
            case e: Exception => log.info(e.printStackTrace())
        }
	      }

    def getDataFrame(spark: SparkSession, hdfs_nasa: String): DataFrame = {
        import spark.implicits._
        //val nasa_df = spark.sparkContext.textFile(hdfs_nasa)
        val separator = " "
        val nasa_df = spark.read.option("delimiter", separator).csv(hdfs_nasa)
        val _df = nasa_df.toDF.withColumnRenamed("_c0", "host_request")
                              .withColumn("time_stamp", AccessFunc.to_timestamp_udf(col("_c3"),col("_c4")))
                              .withColumnRenamed("_c5", "request")
                              .withColumnRenamed("_c6", "codigo_http")
                              .withColumnRenamed("_c7", "bytes_return")
        _df.select("host_request", "time_stamp", "request", "codigo_http", "bytes_return")
    }

    def host_unique(spark: SparkSession, df: DataFrame): Unit = {
        log.info(s"*** 1 -> Número de hosts únicos.")
        log.info(s"**********************************************************************************")
        import spark.implicits._
        val _host_unique_df = df.select("host_request")
                                .groupBy("host_request")
                                .agg(count("host_request"))
        val _df = List((_host_unique_df.count)).toDF("host_unique")
        _df.show(false)
    }

    def total_404(spark: SparkSession, df: DataFrame): Unit = {
        log.info(s"*** 2 ->  O total de erros 404.")
        log.info(s"**********************************************************************************")
        import spark.implicits._
        val _total_404_df = df.select("host_request")
                              .where("codigo_http=404")
        val _df = List((_total_404_df.count)).toDF("total_404")
        _df.show(false)
    }

    def top_5_404(spark: SparkSession, df: DataFrame): Unit = {
        log.info(s"*** 3 ->  Os 5 URLs que mais causaram erro 404.")
        log.info(s"**********************************************************************************")
        import spark.implicits._
        val _top_5_404 = df.select("host_request", "codigo_http")
                            .where("codigo_http=404")
                            .groupBy("host_request", "codigo_http")
                            .agg(count("host_request") as "quantity")
                            .orderBy(col("quantity").desc)
        _top_5_404.show(5,false)
    }

    def quantity_404_day(spark: SparkSession, df: DataFrame): Unit = {
        log.info(s"*** 4 ->  Quantidade de erros 404 por dia, ordernado quantity desc.")
        log.info(s"**********************************************************************************")
        val _df = df.transform(with_data())
        val _quantity_404_day_df = _df.select("host_request", "data", "codigo_http")
                                     .where("codigo_http=404")
                                     .groupBy("host_request", "data", "codigo_http")
                                     .agg(count("host_request") as "quantity")
                                     .orderBy(col("quantity").desc)
        _quantity_404_day_df.show(10,false)
    }

    def total_bytes(spark: SparkSession, df: DataFrame): Unit = {
        log.info(s"*** 5 ->  O total de bytes retornados.")
        log.info(s"**********************************************************************************")
        val _total_bytes = df.agg(sum("bytes_return") as "total_bytes")
        _total_bytes.show(false)
    }

    def with_data()(df: DataFrame): DataFrame = {
        // with data​ ​no formato "DIA/MÊS/ANO TIMEZONE to group"
        val _df = df.withColumn("data", AccessFunc.to_date_udf(col("time_stamp")))
        _df
    }

    def with_host_request()(df: DataFrame): DataFrame = {
        // Host fazendo a requisição​. Um hostname quando possível,
        // caso contrário o endereço de internet se o nome não puder ser identificado
        val _df = df.withColumn("host_request", col("host_request"))
        df
    }

    def with_time_stamp()(df: DataFrame): DataFrame = {
        // Timestamp​ ​no formato "DIA/MÊS/ANO:HH:MM:SS TIMEZONE"
        val _df = df.withColumn("time_stamp", col("time_stamp"))
        _df
    }

    def with_request()(df: DataFrame): DataFrame = {
        // Requisição​ ​(entre​ ​aspas)
        val _df = df.withColumn("request", col("request"))
        _df
    }

    def with_codigo_http()(df: DataFrame): DataFrame = {
        // Código​ ​do​ ​retorno​ ​HTTP
        val _df = df.withColumn("codigo_http", col("codigo_http"))
        _df
    }

    def with_bytes_return()(df: DataFrame): DataFrame = {
        // Total​ ​de​ ​bytes​ ​retornados
        val _df = df.withColumn("bytes_return", col("bytes_return"))
        _df
    }
}