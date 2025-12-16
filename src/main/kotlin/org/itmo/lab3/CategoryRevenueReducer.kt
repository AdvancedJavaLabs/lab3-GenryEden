package org.itmo.lab3

import java.util.Locale
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class CategoryRevenueReducer : Reducer<Text, CategoryStatsWritable, Text, Text>() {
    private val aggregated = mutableMapOf<String, CategoryStatsWritable>()
    private val outValue = Text()

    override fun reduce(key: Text, values: Iterable<CategoryStatsWritable>, context: Context) {
        val revenue = values.sumOf { it.revenue }
        val quantity = values.sumOf { it.quantity }

        aggregated[key.toString()] = CategoryStatsWritable(revenue, quantity)
    }

    override fun cleanup(context: Context) {
        val sorted = aggregated.entries.sortedWith(
                compareByDescending<Map.Entry<String, CategoryStatsWritable>> { it.value.revenue }
                    .thenBy { it.key },
            )

        for ((category, stats) in sorted) {
            val revenueStr = String.format(Locale.US, "%.2f", stats.revenue)
            outValue.set("$revenueStr\t${stats.quantity}")
            context.write(Text(category), outValue)
        }
    }
}
