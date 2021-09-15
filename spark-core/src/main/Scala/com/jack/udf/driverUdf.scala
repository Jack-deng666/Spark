package com.jack.udf

import java.io.{BufferedReader, InputStreamReader, ObjectOutputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author jack Deng
 * @date 2021/9/15 11:20
 * @version 1.0
 */
object driverUdf {
  def main(args: Array[String]): Unit = {
    val task = new Task()

    val socket = new Socket("localhost", 1024)
    val out = socket.getOutputStream
    val objectOut = new ObjectOutputStream(out)
    val subtask = new SubTask()
    subtask.data = task.data.take(2)
    subtask.login_copy = task.login_copy
    objectOut.writeObject(subtask)
    objectOut.flush()
    objectOut.close()
    socket.close()
    print("发送计算方式")


    val socket1 = new Socket("localhost", 2048)
    val out1 = socket1.getOutputStream
    val objectOut1 = new ObjectOutputStream(out1)
    val subtask1 = new SubTask()
    subtask1.data = task.data.takeRight(2)
    subtask1.login_copy = task.login_copy
    objectOut1.writeObject(subtask1)
    objectOut1.flush()
    objectOut1.close()
    socket1.close()
    print("发送计算方式")


  }
}
