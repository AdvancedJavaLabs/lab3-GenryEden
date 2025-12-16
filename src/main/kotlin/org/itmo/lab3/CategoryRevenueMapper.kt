package org.itmo.lab3

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class CategoryRevenueMapper : Mapper<LongWritable, Text, Text, CategoryStatsWritable>() {
    private val reusableKey = Text()

    override fun map(key: LongWritable, value: Text, context: Context) {
        val line = value.toString().trim()
        if (line.isEmpty() || line.startsWith("transaction_id", ignoreCase = true)) return

        val parts = line.split(',')
        if (parts.size < 5) return

        val category = parts[2].trim()
        val price = parts[3].toDoubleOrNull() ?: return
        val quantity = parts[4].toLongOrNull() ?: return

        reusableKey.set(category)
        context.write(reusableKey, CategoryStatsWritable(price * quantity, quantity))
    }
}
