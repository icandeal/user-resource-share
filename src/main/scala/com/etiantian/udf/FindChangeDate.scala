package com.etiantian.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructType, StructField, DataType, IntegerType, StringType}

class FindChangeDate extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(Seq(
    StructField("share_status", IntegerType),
    StructField("c_time", StringType)
  ))

  /**
    * share_status 3 仅自己，其他是共享
    * @return
    */

  override def bufferSchema: StructType = StructType(Seq(
    StructField("status", IntegerType),
    StructField("c_time", StringType)
  ))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 3
    buffer(1) = "1971-01-01 00:00:00"
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.getInt(0) != 3){    //新记录共享
      if (buffer.getInt(0) == 3) {   //旧记录不共享，替换
        buffer(0) = input(0)
        buffer(1) = input(1)
      }
      else {                        //旧记录共享， 取小的
        if (buffer.getString(1).compareTo(input.getString(1)) > 0) {
          buffer(0) = input(0)
          buffer(1) = input(1)
        }
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer2.getInt(0) != 3){    //新记录共享
      if (buffer1.getInt(0) == 3) {   //旧记录不共享，替换
        buffer1(0) = buffer2(0)
        buffer1(1) = buffer2(1)
      }
      else {                        //旧记录共享， 取小的
        if (buffer1.getString(1).compareTo(buffer2.getString(1)) > 0) {
          buffer1(0) = buffer2(0)
          buffer1(1) = buffer2(1)
        }
      }
    }
  }

  override def evaluate(buffer: Row): Any = {
    if (buffer.getInt(0) == 3) {
      null
    }
    else {
      buffer.getString(1).substring(0,10)
    }
  }
}
