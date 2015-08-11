package ch02

import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, LongWritable}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.JavaConverters._

class MaxTemperatureMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  private final val MISSING: Int = 9999

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val line: String = value.toString
    val year: String = line.substring(15, 19)
    var airTemperature: Int = 0
    if (line.charAt(87) == '+') {
      airTemperature = line.substring(88, 92).toInt
    }
    else {
      airTemperature = line.substring(87, 92).toInt
    }
    val quality: String = line.substring(92, 93)
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(airTemperature))
    }
  }
}


class MaxTemperatureReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    val newValues = values.asScala
    val maxValue = if (newValues.isEmpty) Integer.MIN_VALUE else (newValues map { _.get } max)

    context.write(key, new IntWritable(maxValue))
  }
}

object MaxTemperature {
  def main(rawArgs: Array[String]): Unit = {
    val conf = new Configuration()
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()

    if (args.length != 2) {
      println("Usage: MaxTemperature <input path> <output path>")
      sys.exit(-1)
    }

    val job = new Job(conf, "Hadoop book ch02 MaxTemperature example in Scala")
    job.setJarByClass(this.getClass)
    job.setJobName("Max Temperature")

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setMapperClass(classOf[MaxTemperatureMapper])
    job.setReducerClass(classOf[MaxTemperatureReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    sys.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}