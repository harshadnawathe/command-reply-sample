package com.github.harshadnawathe.x.stream.kafka.util

import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.kafka.test.utils.KafkaTestUtils

fun testKafkaTemplate(): KafkaTemplate<String, String> {
    val brokers = System.getProperty("spring.embedded.kafka.brokers")

    val producerProps = KafkaTestUtils.producerProps(brokers).apply {
        set(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        set(VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer::class.java)
    }

    return KafkaTemplate(
        DefaultKafkaProducerFactory(producerProps)
    )
}
