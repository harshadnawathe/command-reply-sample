package com.github.harshadnawathe.x.stream.integration

import com.github.harshadnawathe.x.stream.config.num.Num
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
        "test.listener.topics=even-nums,odd-nums"
    ]
)
class EvenOddServiceStreamTest {

    @Autowired
    lateinit var listener: KafkaTestListener

    private val kafka by lazy {
        testKafkaTemplate()
    }

    @Test
    fun `should send segregated nums to respective topics`() {

        (1..2).forEach {
            kafka.send(
                MessageBuilder.withPayload(Num(num = it))
                    .setHeader(KafkaHeaders.TOPIC, "nums")
                    .build()
            )
        }

        listener.expect<Num>(onTopic = "even-nums").also {
            assertThat(it.num).isEqualTo(2)
        }

        listener.expect<Num>(onTopic = "odd-nums").also {
            assertThat(it.num).isEqualTo(1)
        }

    }
}