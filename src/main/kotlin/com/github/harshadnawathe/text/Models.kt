package com.github.harshadnawathe.text

import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message

data class Input(
    val text: String
)

data class Output(
    val text: String
)