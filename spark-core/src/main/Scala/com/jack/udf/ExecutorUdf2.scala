package com.jack.udf

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @author jack Deng
 * @date 2021/9/15 11:33
 * @version 1.0
 */
object ExecutorUdf2 {
  def main(args: Array[String]): Unit = {
    val socket = new ServerSocket(2048)
    val server = socket.accept()
    val stream1 = server.getInputStream

    val streamObject = new ObjectInputStream(stream1)
    val task:SubTask = streamObject.readObject().asInstanceOf[SubTask]
    val value:List[Int] = task.compute()

    println("接收到客户端的数据:"+value)
    streamObject.close()
    stream1.close()
    server.close()
    socket.close()
  }
}
