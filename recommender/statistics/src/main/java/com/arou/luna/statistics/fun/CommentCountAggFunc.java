package com.arou.luna.statistics.fun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class CommentCountAggFunc implements AggregateFunction<Tuple3<Long, Double, Long>, Tuple2<Double, Long>, Tuple2<Long, Double>> {



    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return new Tuple2<>(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(Tuple3<Long, Double, Long> t1, Tuple2<Double, Long> t2) {
        return new Tuple2<>(t1.f1 + t2.f1, 1L);
    }

    @Override
    public Tuple2<Long, Double> getResult(Tuple2<Double, Long> t1) {
        return Tuple2.of(t1.f1, t1.f0 / t1.f1);
    }

    //    @Override
//    public Tuple2<Long,Double>> getResult(Tuple2<Double, Long> t1) {
//        return ;
//    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> t1, Tuple2<Double, Long> t2) {
        return Tuple2.of(t1.f0 + t2.f0, t1.f1 + t2.f1);
    }
}
