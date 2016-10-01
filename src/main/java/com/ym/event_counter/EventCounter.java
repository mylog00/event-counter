package com.ym.event_counter;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.*;

/**
 * Объект для учета однотипных событий в системе.
 * Например, отправка фото в сервисе фотографий.
 * События поступают в произвольный момент времени.
 * Возможно как 10К событий в секунду так и 2 в час.
 *
 * @author Dmitry
 * @since 01.10.2016
 */
public class EventCounter implements IEventCounter {

    private final ScheduledExecutorService executorService;

    private final AtomicInteger minuteEventCounter = new AtomicInteger();
    private final AtomicInteger hourEventCounter = new AtomicInteger();
    private final AtomicInteger dayEventCounter = new AtomicInteger();

    public EventCounter() {
        final int processors = Runtime.getRuntime().availableProcessors();
        executorService = Executors.newScheduledThreadPool(processors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean registerEvent() {
        final TimerTask timerTask = new TimerTask(MINUTES);
        minuteEventCounter.incrementAndGet();
        try {
            executorService.schedule(timerTask, 1, MINUTES);
        } catch (RejectedExecutionException e) {
            minuteEventCounter.decrementAndGet();
            return false;
        }
        return true;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getEventsPerMinute() {
        return minuteEventCounter.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEventsPerHour() {
        return minuteEventCounter.get() + hourEventCounter.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEventsPerDay() {
        return minuteEventCounter.get()
                + hourEventCounter.get()
                + dayEventCounter.get();
    }

    private class TimerTask implements Runnable {
        private final TimeUnit timeUnit;

        TimerTask(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public void run() {
            final TimerTask nextTimerTask;
            switch (timeUnit) {
                case MINUTES:
                    nextTimerTask = new TimerTask(HOURS);
                    minuteEventCounter.decrementAndGet();
                    hourEventCounter.incrementAndGet();
                    try {
                        executorService.schedule(nextTimerTask, 59, MINUTES);
                    } catch (RejectedExecutionException ex) {
                        hourEventCounter.decrementAndGet();
                    }
                    break;
                case HOURS:
                    nextTimerTask = new TimerTask(DAYS);
                    hourEventCounter.decrementAndGet();
                    dayEventCounter.incrementAndGet();
                    try {
                        executorService.schedule(nextTimerTask, 23, HOURS);
                    } catch (RejectedExecutionException ex) {
                        dayEventCounter.decrementAndGet();
                    }
                    break;
                case DAYS:
                    dayEventCounter.decrementAndGet();
            }
        }
    }
}
