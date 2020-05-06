package com.arou.luna.statistics;

import com.arou.luna.statistics.model.Ratting;
import com.arou.luna.statistics.sink.AvgScoreSinkToMongo;
import com.arou.luna.statistics.sink.TimeCommentCountSinkToMongo;
import com.arou.luna.statistics.sink.TotalCommentCountSinkToMongo;
import com.arou.luna.statistics.source.RattingMongoSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StatisticsRecommend {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Ratting> streamSource = env.addSource(new RattingMongoSourceFunction()).setParallelism(1);

//        totalCommentCount(streamSource);
//        avgScore(streamSource);
        commentCountByTime(streamSource);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void commentCountByTime(DataStream<Ratting> streamSource) {
        streamSource
                .map(new MapFunction<Ratting, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> map(Ratting ratting) throws Exception {
                        return Tuple3.of(ratting.getProductId(),
                                new SimpleDateFormat("yyyyMM").format(new Date(ratting.getTimestamp() * 1000)),
                                1L);
                    }
                })
                .keyBy(1)
                .keyBy(0)
                .sum(2)
                .addSink(new TimeCommentCountSinkToMongo());

    }

    /**
     * 商品评分平均值
     *
     * @param streamSource
     */
    private static void avgScore(DataStream<Ratting> streamSource) {
        streamSource
                .map(new MapFunction<Ratting, Tuple3<Long, Double, Long>>() {
                    @Override
                    public Tuple3<Long, Double, Long> map(Ratting ratting) throws Exception {
                        return new Tuple3<>(ratting.getProductId(), ratting.getScore(), 1L);
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<Long, Double, Long>>() {
                    @Override
                    public Tuple3<Long, Double, Long> reduce(Tuple3<Long, Double, Long> t0, Tuple3<Long, Double, Long> t1) throws Exception {
                        return Tuple3.of(t0.f0, (t0.f1 + t1.f1) / 2, t0.f2 + t1.f2);
                    }
                })
                .addSink(new AvgScoreSinkToMongo());
    }

    /**
     * 每个商品评论总数
     *
     * @param streamSource
     */
    private static void totalCommentCount(DataStream<Ratting> streamSource) {
        //1. 历史热门商品，按照评分个数统计，productId，count
        streamSource
                .map(new MapFunction<Ratting, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Ratting ratting) throws Exception {
                        return new Tuple2<>(ratting.getProductId(), 1L);
                    }
                })
                .keyBy(0)
                .sum(1)
                .addSink(new TotalCommentCountSinkToMongo());
    }
}
