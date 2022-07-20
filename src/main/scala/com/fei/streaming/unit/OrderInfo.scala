package com.fei.streaming.unit

/**
  * @Description:
  * @ClassName: OrderInfo
  * @Author chengfei
  * @DateTime 2022/7/20 13:52
  **/
case class OrderInfo(oid: String, status: String, payId: String, actionTime: Long, endTime: Long)
