package com.fei.streaming.source

import com.fei.streaming.ESSourceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
  * @Description:
  * @ClassName: ESPallSource
  * @Author chengfei
  * @DateTime 2022/7/13 16:28
  **/
object ESParallellSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //String hosts, String username, String password, String index, String document_type, Integer fetch_size, DeserializationSchema<RowData> deserializer
    val dataStream: DataStream[String] = env.addSource(new ESSourceFunction("http://10.191.80.158:9200", "", ""
      , "h5_wl_register_xgame", "_doc", 2, new SimpleStringSchema())).setParallelism(3)
    dataStream.print()
    env.execute()
  }
}
