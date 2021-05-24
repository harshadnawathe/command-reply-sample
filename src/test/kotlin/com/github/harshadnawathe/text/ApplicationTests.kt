package com.github.harshadnawathe.text

import com.github.harshadnawathe.text.util.KafkaTestListener
import com.github.harshadnawathe.text.util.testKafkaTemplateForStringValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.messaging.support.MessageBuilder
import org.springframework.test.context.TestPropertySource
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka(partitions = 1)
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "test.listener.topics=client-reply-1,client-reply-2"
    ]
)
class ApplicationTests {

    @Autowired
    lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker

    @Autowired
    lateinit var listener: KafkaTestListener

    private val kafka
        get() = testKafkaTemplateForStringValue(embeddedKafkaBroker)

    @BeforeEach
    internal fun setUp() {
        listener.reset(2)
    }
    @Test
    fun `should consume Input and produce reversed Output`() {
        val latch = listener.latch()

        kafka.send(
            MessageBuilder.withPayload(Input(text = "Hello"))
                .setHeader(KafkaHeaders.TOPIC, "svc-in-text-reverse")
                .setHeader(KafkaHeaders.REPLY_TOPIC, "client-reply-1")
                .build()
        )
        kafka.send(
            MessageBuilder.withPayload(Input(text = "Hi"))
                .setHeader(KafkaHeaders.TOPIC, "svc-in-text-reverse")
                .setHeader(KafkaHeaders.REPLY_TOPIC, "client-reply-2")
                .build()
        )
        latch.await(30, TimeUnit.SECONDS)

        assertThat(latch.count).isEqualTo(0)
        assertThat(
            listener.messages()
        ).anyMatch {
            it.payload == "{\"text\":\"olleH\"}" && it.headers[KafkaHeaders.RECEIVED_TOPIC] == "client-reply-1"
        }.anyMatch {
            it.payload == "{\"text\":\"iH\"}" && it.headers[KafkaHeaders.RECEIVED_TOPIC] == "client-reply-2"
        }


    }

}
