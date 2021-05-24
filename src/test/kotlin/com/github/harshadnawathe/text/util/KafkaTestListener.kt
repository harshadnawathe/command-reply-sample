package com.github.harshadnawathe.text.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.stereotype.Component
import java.util.concurrent.CountDownLatch

@Component
class KafkaTestListener {

    private var latch: CountDownLatch = CountDownLatch(1)
    fun latch() = latch

    private val messagesReceived = mutableListOf<Message<String>>()

    @KafkaListener(
        topics = ["#{'\${test.listener.topics:''}'.split(',')}"],
        groupId = "kafka-test-listener-group"
    )
    fun handle(m: Message<String>) {
        LOG.info("Received ${m.payload} on topic ${m.headers[KafkaHeaders.RECEIVED_TOPIC]}")
        messagesReceived += m
        latch.countDown()
    }

    fun reset(n: Int = 1) {
        latch = CountDownLatch(n)
        messagesReceived.clear()
    }

    fun messages() : List<Message<String>> {
        return messagesReceived.toList()
    }

    companion object {
        @JvmStatic
        val LOG : Logger = LoggerFactory.getLogger(KafkaTestListener::class.java)
    }
}