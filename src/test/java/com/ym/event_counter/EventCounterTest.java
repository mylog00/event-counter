package com.ym.event_counter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Unit test for EventCounter.
 */
public class EventCounterTest {

    private IEventCounter eventCounter;


    @Before
    public void prepare() {
        eventCounter = new EventCounter();
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

        Thread.sleep(TimeUnit.SECONDS.toMillis(61));

        Assert.assertEquals(0, eventCounter.getEventsPerMinute());
        Assert.assertEquals(3, eventCounter.getEventsPerHour());
        Assert.assertEquals(3, eventCounter.getEventsPerDay());
    }


}
