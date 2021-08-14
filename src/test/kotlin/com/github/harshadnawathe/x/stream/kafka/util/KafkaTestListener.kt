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
import java.util.concurrent.TimeUnit.SECONDS

@Component
class KafkaTestListener(
    private val mapper: ObjectMapper,
    private val table: TopicToQueueTable
) {
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
        table.queueFor(record.topic()).add(record)
    }

    fun next(topic: String, timeout: Timeout): ConsumerRecord<String, String>? =
        table.queueFor(topic).poll(timeout.time, timeout.unit)

    fun <T> ConsumerRecord<String, String>.mapTo(type: Class<T>): T = mapper.readValue(value(), type)

    companion object {
        @JvmStatic
        val LOG: Logger = LoggerFactory.getLogger(KafkaTestListener::class.java)
    }
}

inline fun <reified T> KafkaTestListener.expect(
    onTopic: String,
    timeout: Timeout = within(10, SECONDS),
    check: (T) -> Unit = {}
): T {
    return next(onTopic, timeout)
        ?.mapTo(T::class.java)
        ?.also(check)
        ?: fail {
            "Did not receive event ${T::class.java.name} on $onTopic"
        }
}
