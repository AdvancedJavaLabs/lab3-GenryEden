package org.itmo.lab3

data class CliArgs(
    val inputPath: String,
    val outputPath: String,
    val mapThreads: Int = 4,
    val reducers: Int = 1,
)


