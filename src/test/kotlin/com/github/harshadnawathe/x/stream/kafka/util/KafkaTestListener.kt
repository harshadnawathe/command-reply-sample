package com.github.harshadnawathe.x.stream.kafka.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.fail
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@Component
class KafkaTestListener(
    private val mapper: ObjectMapper
) {
    private val topicToQueue = ConcurrentHashMap<String, LinkedBlockingQueue<ConsumerRecord<String, String>>>()

    @KafkaListener(
        id = "kafkaTestListener",
        topics = ["#{'\${test.listener.topics:''}'.split('\\s*,\\s*')}"],
        groupId = "kafka-test-listener-group",
        properties = [
            "$KEY_DESERIALIZER_CLASS_CONFIG=org.apache.kafka.common.serialization.StringDeserializer",
            "$VALUE_DESERIALIZER_CLASS_CONFIG=org.apache.kafka.common.serialization.StringDeserializer"
        ]
    )
    private fun enqueue(record: ConsumerRecord<String, String>) {
        LOG.info("Received ${record.value()} on topic ${record.topic()}")
        queueFor(record.topic()).add(record)
    }

    fun next(topic: String, timeout: Long, unit: TimeUnit): ConsumerRecord<String, String>? =
        queueFor(topic).poll(timeout, unit)

    fun <T> ConsumerRecord<String, String>.mapTo(type: Class<T>): T {
        return mapper.readValue(value(), type)
    }

    private fun queueFor(topic: String) = topicToQueue.computeIfAbsent(topic) {
        LOG.info("Added queue for $topic")
        LinkedBlockingQueue()
    }

    companion object {
        @JvmStatic
        val LOG: Logger = LoggerFactory.getLogger(KafkaTestListener::class.java)
    }
}

inline fun <reified T> KafkaTestListener.expect(
    onTopic: String,
    timeout: Long = 10,
    unit: TimeUnit = TimeUnit.SECONDS,
    check: (T) -> Unit = {}
): T {
    return next(onTopic, timeout, unit)
        ?.mapTo(T::class.java)
        ?.also(check)
        ?: fail {
            "Did not receive event ${T::class.java.name} on $onTopic"
        }
}