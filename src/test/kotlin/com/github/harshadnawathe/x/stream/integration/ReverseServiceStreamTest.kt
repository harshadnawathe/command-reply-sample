package com.github.harshadnawathe.x.stream.integration

import com.github.harshadnawathe.x.stream.kafka.util.KafkaTestListener
import com.github.harshadnawathe.x.stream.kafka.util.expect
import com.github.harshadnawathe.x.stream.kafka.util.testKafkaTemplate
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.messaging.support.MessageBuilder
import org.springframework.test.context.TestPropertySource

@SpringBootTest
@EmbeddedKafka(partitions = 1)
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "test.listener.topics=client-reply-1,client-reply-2"
    ]
)
class ReverseServiceStreamTest {

    @Autowired
    lateinit var listener: KafkaTestListener

    private val kafka by lazy {
        testKafkaTemplate()
    }

    @Test
    fun `should consume Input and produce reversed Output`() {
        kafka.send(
            MessageBuilder.withPayload(InputMessage(text = "Hello"))
                .setHeader(KafkaHeaders.TOPIC, "svc-in-text-reverse")
                .setHeader(KafkaHeaders.REPLY_TOPIC, "client-reply-1")
                .build()
        )
        kafka.send(
            MessageBuilder.withPayload(InputMessage(text = "Hi"))
                .setHeader(KafkaHeaders.TOPIC, "svc-in-text-reverse")
                .setHeader(KafkaHeaders.REPLY_TOPIC, "client-reply-2")
                .build()
        )

        listener.expect<OutputMessage>(onTopic = "client-reply-1").also {
            assertThat(it.text).isEqualTo("olleH")
        }

        listener.expect<OutputMessage>(onTopic = "client-reply-2").also {
            assertThat(it.text).isEqualTo("iH")
        }
    }
}

data class InputMessage(val text: String)
data class OutputMessage(val text: String)
