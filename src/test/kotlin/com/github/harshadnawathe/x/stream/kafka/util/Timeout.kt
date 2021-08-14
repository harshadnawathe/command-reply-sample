package com.github.harshadnawathe.x.stream.kafka.util

import java.util.concurrent.TimeUnit

data class Timeout(val time: Long, val unit: TimeUnit)

fun within(time: Long, unit: TimeUnit) = Timeout(time, unit)