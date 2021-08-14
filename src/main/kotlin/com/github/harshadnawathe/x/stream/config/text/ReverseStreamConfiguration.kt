package com.github.harshadnawathe.x.stream.config.text

import com.github.harshadnawathe.x.stream.text.reverse.Input
import com.github.harshadnawathe.x.stream.text.reverse.Output
import com.github.harshadnawathe.x.stream.text.reverse.ReverseService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import reactor.core.publisher.Flux
import java.util.function.Function

@Configuration
class ReverseStreamConfiguration {

    @Bean
    fun reverse(service: ReverseService) =
        Function<Flux<Message<Input>>, Flux<Message<Output>>> { inputStream ->
            inputStream.concatMap { m ->
                service.reverse(m.payload).map { reversed ->
                    Exchange(reversed, m)
                }
            }.map { exchange ->
                exchange.response()
            }
        }

    private class Exchange(
        private val output: Output,
        input: Message<Input>
    ) {

        private val replyTopic: String? = input.headers.get(KafkaHeaders.REPLY_TOPIC, String::class.java)
        fun response(): Message<Output> =
            with(MessageBuilder.withPayload(output)) {
                if (replyTopic != null) {
                    setHeader("spring.cloud.stream.sendto.destination", replyTopic)
                }
                build()
            }
    }
}


