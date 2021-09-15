package com.jack.udf

/**
 * @author jack Deng
 * @date 2021/9/15 13:48
 * @version 1.0
 */
class SubTask extends Serializable {
    var data:List[Int] = _
    var login_copy:(Int) => Int =  _

  def compute():List[Int] ={
    data.map(login_copy)
  }

}
