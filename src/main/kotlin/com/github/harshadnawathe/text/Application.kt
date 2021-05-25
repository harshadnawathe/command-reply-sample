package com.github.harshadnawathe.text

import com.github.harshadnawathe.text.reverse.ReverseService
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import reactor.core.publisher.Flux
import java.util.function.Function

class Exchange(
    private val output: Output,
    input: Message<Input>
) {
    private val replyTopic: String? = input.headers.get(KafkaHeaders.REPLY_TOPIC, String::class.java)

    fun response(): Message<Output> {
        val mb = MessageBuilder.withPayload(output)
        replyTopic?.also { destination ->
            mb.setHeader("spring.cloud.stream.sendto.destination", destination)
        }
        return mb.build()
    }
}

@SpringBootApplication
class Application {

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
}

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}
