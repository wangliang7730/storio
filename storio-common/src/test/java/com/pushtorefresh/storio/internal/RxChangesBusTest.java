package com.pushtorefresh.storio.internal;

import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.internal.util.RxRingBuffer;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class RxChangesBusTest {

    /**
     * Increments int without backpressure..
     */
    private static Observable<Integer> firehose(final AtomicInteger counter) {
        return Observable.create(new Observable.OnSubscribe<Integer>() {

            int i = 0;

            @Override
            public void call(final Subscriber<? super Integer> s) {
                while (!s.isUnsubscribed()) {
                    s.onNext(i);
                    counter.incrementAndGet();
                }
            }
        });
    }

    final static Func1<Integer, Integer> SLOW_PASS_THRU = new Func1<Integer, Integer>() {
        volatile int sink;

        @Override
        public Integer call(Integer t1) {
            // be slow ... but faster than Thread.sleep(1)
            String t = "";
            int s = sink;
            for (int i = 1000; i >= 0; i--) {
                t = String.valueOf(i + t.hashCode() + s);
            }
            sink = t.hashCode();
            return t1;
        }

    };

    @Test(timeout = 2000)
    public void onBackpressureBufferAndMerge() {
        final int NUM = (int) (RxRingBuffer.SIZE * 1.1); // > 1 so that take doesn't prevent buffer overflow

        final AtomicInteger emitted = new AtomicInteger();
        TestSubscriber<Integer> ts = new TestSubscriber<Integer>();

        final LinkedHashSet<Integer> uniqueOrderedEmission = new LinkedHashSet<Integer>();

        Observable.create(new Observable.OnSubscribe<Integer>() {
            int i = 0;

            @Override
            public void call(final Subscriber<? super Integer> s) {
                while (!s.isUnsubscribed()) {
                    int result;

                    if (i % 10 == 0) { // Each 10 item will be non unique, so in total 10 non-unique items per 100 items, 100 non-unique per 1000 and so on.
                        result = i / 10;
                    } else {
                        result = i;
                    }

                    s.onNext(result);
                    emitted.incrementAndGet();
                    i++;
                }
            }
        })
                .doOnNext(new Action1<Integer>() {
                    int countOfAddedItems = 0;

                    @Override
                    public void call(Integer integer) {
                        if (countOfAddedItems < NUM) {
                            if (uniqueOrderedEmission.add(integer)) {
                                countOfAddedItems++;
                            }
                        }
                    }
                })
                .takeWhile(new Func1<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer integer) {
                        return integer < 10000;
                    }
                })
                .lift(OperatorOnBackpressureBufferAndMerge.<Integer>instance())
                .observeOn(Schedulers.computation())
                .map(SLOW_PASS_THRU)
                .take(NUM)
                .subscribe(ts);

        ts.awaitTerminalEvent();
        ts.assertNoErrors();
        System.out.println("testOnBackpressureBuffer => Received: " + ts.getOnNextEvents().size() + "  Emitted: " + emitted.get());

        assertEquals(NUM, ts.getOnNextEvents().size());
        assertEquals(uniqueOrderedEmission.size(), ts.getOnNextEvents().size());

        int i = 0;
        for (Integer expectedItem : uniqueOrderedEmission) {
            assertEquals(expectedItem, ts.getOnNextEvents().get(i++));
        }
    }

    @Test
    public void onNextShouldSendMessagesToObserver() {
        RxChangesBus<String> rxChangesBus = new RxChangesBus<String>();

        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        rxChangesBus
                .asObservable()
                .subscribe(testSubscriber);

        List<String> messages = asList("yo", ",", "wanna", "some", "messages?");

        for (String message : messages) {
            rxChangesBus.onNext(message);
        }

        testSubscriber.assertReceivedOnNext(messages);

        testSubscriber.assertNoErrors();
        testSubscriber.assertNoTerminalEvent();
    }

    @Test
    public void backpressure() throws InterruptedException {
        final int threadsCount = 20;
        final int emissionsPerThread = 1000000;

        final RxChangesBus<String> rxChangesBus = new RxChangesBus<String>();
        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        rxChangesBus
                .asObservable()
                .filter(new Func1<String, Boolean>() {
                    @Override
                    public Boolean call(String s) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                        }

                        return true;
                    }
                })
                .observeOn(Schedulers.newThread())
                .subscribe(testSubscriber);

        final CountDownLatch countDownLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int emission = 0; emission < emissionsPerThread; emission++) {
                        rxChangesBus.onNext("t" + threadsCount + "e" + emission);
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }

        countDownLatch.await(5, MINUTES);
        testSubscriber.assertNoTerminalEvent();

        assertThat(testSubscriber.getOnNextEvents()).hasSize(threadsCount * emissionsPerThread);
    }
}
