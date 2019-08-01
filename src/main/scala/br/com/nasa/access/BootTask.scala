package br.com.nasa.access
import java.util.Properties
import java.io.FileInputStream


trait BootTask extends Serializable {
    private var _fs_name = ""
    private var _job_queuename = ""
    private var _fs_nasa = ""

    //getters
    def fs_name = _fs_name
    def job_queuename = _job_queuename
    def fs_nasa = _fs_nasa

    //setters
    def fs_name_(value:String):Unit = _fs_name = value
    def job_queuename_(value:String):Unit = _job_queuename = value
    def fs_nasa_(value:String):Unit = _fs_nasa = value

    def readProperties(config_file: String) {
        val prop = new Properties()
        val input = new FileInputStream( config_file )
        prop.load( input )
        fs_name_( prop.getProperty( "fs_name" ) )
        job_queuename_( prop.getProperty( "job_queuename" ) )
        fs_nasa_( prop.getProperty( "fs_nasa" ) )
    }

}
