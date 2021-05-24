package com.github.harshadnawathe.text.reverse

import com.github.harshadnawathe.text.Input
import com.github.harshadnawathe.text.Output
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