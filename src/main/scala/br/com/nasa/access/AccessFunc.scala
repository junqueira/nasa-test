package br.com.nasa.access
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, max}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object  AccessFunc {

    val to_date_udf: UserDefinedFunction = udf[String, String](get_to_date)
    def get_to_date(timestamp: String): String = {
        // tranform time_stamp to date FORMAT 'MM/DD/YYYY to group'
        try {
            timestamp.replace("[","")
                     .split(":")(0)
        }
        catch {
            case _: Throwable => timestamp
        }
    }

    val to_timestamp_udf: UserDefinedFunction = udf[String, String, String](get_to_timestamp)
    def get_to_timestamp(timestamp: String, len_ms: String="6"): String = {
        // TIMESTAMP(6) FORMAT 'MM/DD/YYYYBHH:MI:SS.S(6)'
        try {
            timestamp.concat(len_ms)
        }
        catch {
            case _: Throwable => timestamp
        }
    }
}
