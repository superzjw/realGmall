package com.atguigu.utils
import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertiesName:String):Properties ={
    var prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
    prop
  }
}