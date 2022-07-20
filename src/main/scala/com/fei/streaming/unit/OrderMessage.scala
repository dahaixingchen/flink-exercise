package com.fei.streaming.unit

/**
  * @Description:
  * @ClassName: OrderMessage
  * @Author chengfei
  * @DateTime 2022/7/20 14:10
  **/
case class OrderMessage(oid:String,msg:String,createTime:Long,payTime:Long)
