package com.ym.event_counter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for EventCounter.
 */
public class EventCounterTest {

    private IEventCounter eventCounter;
    private ExecutorService executor;


    @Before
    public void prepare() {
        eventCounter = new EventCounter();
        executor = Executors.newFixedThreadPool(15);
    }

    /**
     * Simple minute test
     */
    @Test
    public void testEventCounterAfterMinute() throws InterruptedException {
        eventCounter.registerEvent();
        eventCounter.registerEvent();
        eventCounter.registerEvent();
        Assert.assertEquals(3, eventCounter.getEventsPerMinute());
        Assert.assertEquals(3, eventCounter.getEventsPerHour());
        Assert.assertEquals(3, eventCounter.getEventsPerDay());

        Thread.sleep(TimeUnit.SECONDS.toMillis(59));
        Assert.assertEquals(3, eventCounter.getEventsPerMinute());
        Assert.assertEquals(3, eventCounter.getEventsPerHour());
        Assert.assertEquals(3, eventCounter.getEventsPerDay());

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        Assert.assertEquals(0, eventCounter.getEventsPerMinute());
        Assert.assertEquals(3, eventCounter.getEventsPerHour());
        Assert.assertEquals(3, eventCounter.getEventsPerDay());
    }

    @Test
    public void testMultiThreadEvents() throws InterruptedException {
        final int threadNum = 1000;
        final int iterations = 1000;
        final int allEvents = threadNum * iterations;
        for (int i = 0; i < threadNum; i++) {
            executor.submit(new Loader(eventCounter, iterations));
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(59));
        Assert.assertEquals(allEvents, eventCounter.getEventsPerMinute());
        Assert.assertEquals(allEvents, eventCounter.getEventsPerHour());
        Assert.assertEquals(allEvents, eventCounter.getEventsPerDay());

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        Assert.assertEquals(0, eventCounter.getEventsPerMinute());
        Assert.assertEquals(allEvents, eventCounter.getEventsPerHour());
        Assert.assertEquals(allEvents, eventCounter.getEventsPerDay());
    }

    private static class Loader implements Runnable {
        private final IEventCounter eventCounter;
        private final int counter;

        Loader(final IEventCounter eventCounter, int counter) {
            this.eventCounter = eventCounter;
            this.counter = counter;
        }

        @Override
        public void run() {
            for (int i = 0; i < counter; i++) {
                eventCounter.registerEvent();
            }
        }
    }

}
