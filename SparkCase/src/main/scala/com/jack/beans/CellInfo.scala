package com.jack.beans

/**
 * @param time 数据产生时间
 * @param city_id 城市Id
 * @param city_name 城市名称
 * @param area 区域
 * @param userId 用户Id
 * @param produceId 商品Id
 */

case class CellInfo(
                      time:String,
                      cityId:String,
                      area:String,
                      userId:String,
                      produceId:String
                    ){
  override def toString: String = {
    time + "_" + area + "_" + cityId+ "_" + userId+ "_" + produceId
  }
}

