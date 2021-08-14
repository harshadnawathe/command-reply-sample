package com.github.harshadnawathe.x.stream.kafka.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue

@Component
class TopicToQueueTable {
    private val map = ConcurrentHashMap<String, LinkedBlockingQueue<ConsumerRecord<String, String>>>()

    fun queueFor(topic: String) = map.computeIfAbsent(topic) {
        LOG.info("Added queue for $topic")
        LinkedBlockingQueue()
    }

    companion object {
        @JvmStatic
        val LOG: Logger = LoggerFactory.getLogger(TopicToQueueTable::class.java)
    }
}