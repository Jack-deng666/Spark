package com.jack.base

import scopt.OptionParser

/**
 * @author jack Deng
 * @date 2021/10/28 18:41
 * @version 1.0
 */
object Fly extends App {
  private val parser: OptionParser[Config] = new OptionParser[Config]("paraTest") {
    head("Test-version", "1.0.0")

    opt[String]('t', "name").action((x, c) => c.copy(name = x)).text("name is userName")

    opt[Int]('a', "age").action((x, c) => c.copy(age = x)).text("age is your 年龄")

    help("help").text("this is a help doc")
  }
  parser.parse(args, Config()) match {
    case Some(config)=>println("parse")
    case None =>println("unKnow")
    case  _ => sys.exit(1)
  }




case class Config(name:String = "Jack", age:Int = 24)
}
