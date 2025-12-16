package org.itmo.lab3

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

object SalesByCategoryJob {
    @JvmStatic
    fun main(rawArgs: Array<String>) {
        val args = parseArgs(rawArgs)
        val configuration = Configuration()
        val job = Job.getInstance(configuration, "foobar")

        job.setJarByClass(SalesByCategoryJob::class.java)

        job.mapperClass = MultithreadedMapper::class.java
        MultithreadedMapper.setMapperClass(job, CategoryRevenueMapper::class.java)
        MultithreadedMapper.setNumberOfThreads(job, args.mapThreads)

        job.reducerClass = CategoryRevenueReducer::class.java
        job.numReduceTasks = args.reducers

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = CategoryStatsWritable::class.java

        job.outputKeyClass = Text::class.java
        job.outputValueClass = Text::class.java

        job.inputFormatClass = TextInputFormat::class.java
        job.outputFormatClass = TextOutputFormat::class.java

        FileInputFormat.addInputPath(job, Path(args.inputPath))
        val outputPath = Path(args.outputPath)
        FileSystem.get(configuration).use { fs ->
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true)
            }
        }
        FileOutputFormat.setOutputPath(job, outputPath)

        job.waitForCompletion(true)
    }

    private fun parseArgs(args: Array<String>): CliArgs {
        var inputPath: String? = null
        var outputPath: String? = null
        var mapThreads = 4
        var reducers = 1

        var index = 0
        while (index < args.size) {
            when (args[index]) {
                "--input" -> inputPath = args.getOrNull(++index)
                "--output" -> outputPath = args.getOrNull(++index)
                "--map-threads" -> mapThreads = args.getOrNull(++index)?.toIntOrNull() ?: mapThreads
                "--reducers" -> reducers = args.getOrNull(++index)?.toIntOrNull() ?: reducers
                else -> throw IllegalArgumentException("Unknown argument: ${args[index]}")
            }
            index++
        }

        return CliArgs(
            inputPath = inputPath ?: "",
            outputPath = outputPath ?: "",
            mapThreads = if (mapThreads < 1) 1 else mapThreads,
            reducers = if (reducers < 1) 1 else reducers,
        )
    }
}


