import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by ZhangZaipeng on 2017/8/7 0007.
 * 注意：map阶段的输出的key-value对的格式必须同reduce阶段的输入key-value对的格式相对应
 * eg: a.txt 结构为
 *     Deer Bear River
 *     Car Car River
 *     Deer Car Bear
 *
 *     map过程 通过对每一行字符串的解析
 *     (Deer,1)
 *     (Bear,1)
 *     (River,1)
 *
 *     (Car,1)
 *     (Car,1)
 *     (River,1)
 *
 *     (Deer,1)
 *     (Car,1)
 *     (Bear,1)
 *
 *     reduce过程，将map过程中的输出，按照相同的key将value放到同一个列表中作为reduce的输入
 *     (Bear,1,1)
 *     (Car,1,1,1)
 *     (Deer,1,1)
 *     (River,1,1)
 *
 *     在reduce过程中，统计的key-value作为输出
 *     (Bear,2)
 *     (Car,3)
 *     (Deer,2)
 *     (River,3)
 */
public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 其中key为此行的开头相对于文件的起始位置，value就是此行的字符文本
            // Context记录输出的key和value

            // 把文件的每一行 按照\t进行分割
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                // 以单词为key 单词的个数为value输出
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // reduce函数与map函数基本相同，但value是一个迭代器的形式Iterable<IntWritable> values，
            // 也就是说reduce的输入是一个key对应一组的值的value
            // 将map过程中的输出，按照相同的key将value放到同一个列表中作为reduce的输入

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); //结果例如World, 2
        }
    }
}
