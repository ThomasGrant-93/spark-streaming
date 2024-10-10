package com.stream.spark.events

case class Event(event: String, user_id: String, timestamp: Long, item_id: String, location: String)
