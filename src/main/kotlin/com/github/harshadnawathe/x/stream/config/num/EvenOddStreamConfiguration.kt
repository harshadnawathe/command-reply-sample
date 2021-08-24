package com.github.harshadnawathe.x.stream.config.num

import com.github.harshadnawathe.x.stream.num.EvenOddService
import com.github.harshadnawathe.x.stream.num.NumType
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Flux
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.util.function.Function

class Num(val num: Int)


@Configuration
class EvenOddStreamConfiguration {

    @Bean
    fun evenOdd(service: EvenOddService) =
        Function<Flux<Num>, Tuple2<Flux<Num>, Flux<Num>>> { nums ->
            val types = nums.map {
                service.numType(it.num)
            }.publish().autoConnect(2)

            Tuples.of(
                types.filter { it is NumType.Even }.map { Num(it.num) },
                types.filter { it is NumType.Odd }.map { Num(it.num) },
            )
        }
}