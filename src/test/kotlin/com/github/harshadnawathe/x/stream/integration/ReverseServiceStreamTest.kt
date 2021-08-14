package com.github.harshadnawathe.x.stream.integration

import com.github.harshadnawathe.x.stream.kafka.util.KafkaTestListener
import com.github.harshadnawathe.x.stream.kafka.util.expect
import com.github.harshadnawathe.x.stream.kafka.util.testKafkaTemplateForStringValue
import com.github.harshadnawathe.x.stream.text.reverse.Input
import com.github.harshadnawathe.x.stream.text.reverse.Output
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.test.EmbeddedKafkaBroker
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
    lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker

    @Autowired
    lateinit var listener: KafkaTestListener

    private val kafka
        get() = testKafkaTemplateForStringValue(embeddedKafkaBroker)


    @Test
    fun `should consume Input and produce reversed Output`() {
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

        listener.expect(onTopic = "client-reply-1") { event: Output ->
            assertThat(event.text).isEqualTo("olleH")
        }

        listener.expect(onTopic = "client-reply-2") { event: Output ->
            assertThat(event.text).isEqualTo("iH")
        }
    }
}
