package com.github.harshadnawathe.x.stream.num

import org.springframework.stereotype.Component


sealed class NumType(val num: Int) {
    class Even(num: Int) : NumType(num)
    class Odd(num: Int) : NumType(num)
}

@Component
class EvenOddService {
    fun numType(num: Int): NumType = if (num % 2 == 0) {
        NumType.Even(num)
    } else {
        NumType.Odd(num)
    }
}