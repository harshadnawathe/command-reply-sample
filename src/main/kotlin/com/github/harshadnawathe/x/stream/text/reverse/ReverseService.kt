package com.github.harshadnawathe.x.stream.text.reverse

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class ReverseService {

    fun reverse(input: Input): Mono<Output> = Mono.fromCallable {
        LOG.info("Processing ${input.text}")
        input.text.reversed().let(::Output)
    }

    companion object {

        @JvmStatic
        val LOG = LoggerFactory.getLogger(ReverseService::class.java)
    }
}

data class Input(
    val text: String
)

data class Output(
    val text: String
)