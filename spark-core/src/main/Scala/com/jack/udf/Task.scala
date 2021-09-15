package com.jack.udf

/**
 * @author jack Deng
 * @date 2021/9/15 11:39
 * @version 1.0
 */
class Task extends Serializable {

  val data:List[Int] = List(1,2,3,4)

  val login_copy:(Int) => Int =_*2

  def compute():List[Int] ={
    data.map(login_copy)
  }

}
