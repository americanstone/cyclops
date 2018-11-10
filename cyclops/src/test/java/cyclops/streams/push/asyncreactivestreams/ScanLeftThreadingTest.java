package cyclops.streams.push.asyncreactivestreams;

import cyclops.companion.Monoids;
import cyclops.reactive.ReactiveSeq;
import cyclops.reactive.Spouts;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;


import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ScanLeftThreadingTest {
    protected  ReactiveSeq<Integer> range(int size){

        return Spouts.from(Flux.range(0, size).subscribeOn(Schedulers.fromExecutor(Executors.newFixedThreadPool(100))).flatMap(i->Flux.just(i),100));

    }
    @Test
    public void scan(){
        assertThat(range(1000000).scanLeft(Monoids.intSum).toList(),equalTo(ReactiveSeq.range(0,100_0000).scanLeft(Monoids.intSum).toList()));
    }
}
