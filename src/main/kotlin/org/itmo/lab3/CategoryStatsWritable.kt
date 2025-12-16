package org.itmo.lab3

import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.Writable

class CategoryStatsWritable(
    var revenue: Double = 0.0,
    var quantity: Long = 0L,
) : Writable {

    override fun write(out: DataOutput) {
        out.writeDouble(revenue)
        out.writeLong(quantity)
    }

    override fun readFields(input: DataInput) {
        revenue = input.readDouble()
        quantity = input.readLong()
    }
}


