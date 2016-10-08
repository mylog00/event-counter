package com.ym.event_counter;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * Объект для учета однотипных событий в системе.
 * Например, отправка фото в сервисе фотографий.
 * События поступают в произвольный момент времени.
 * Возможно как 10К событий в секунду так и 2 в час.
 *
 * @author Dmitry
 * @since 01.10.2016
 */
public class EventCounter implements IEventCounter {
    private static final long MINUTE_OFFSET_IN_MILLIS
            = TimeUnit.MINUTES.toMillis(1);
    private static final long HOUR_OFFSET_IN_MILLIS
            = TimeUnit.MINUTES.toMillis(59);
    private static final long DAY_OFFSET_IN_MILLIS
            = TimeUnit.DAYS.toMillis(23);
    /**
     * Планировщик для выполнения потоков.
     */
    private final ExecutorService executorService;
    /**
     * Счетчик количества событий которые выполнялись меньше минуты назад.
     */
    private final AtomicInteger minuteEventCounter;
    /**
     * Очередь хранящая все события которые выполнялись меньше минуты
     */
    private final BlockingQueue<Long> minuteEventQueue;
    /**
     * Счетчик количества событий которые выполнялись больше минуты,
     * но меньше часа назад.
     */
    private final AtomicInteger hourEventCounter;
    /**
     * Очередь хранящая все события которые выполнялись больше минуты,
     * но меньше часа назад.
     */
    private final BlockingQueue<Long> hourEventQueue;
    /**
     * Счетчик количества событий которые выполнялись больше часа,
     * но меньше суток назад.
     */
    private final AtomicInteger dayEventCounter;
    /**
     * Очередь хранящая все события которые выполнялись больше часа,
     * но меньше суток назад.
     */
    private final BlockingQueue<Long> dayEventQueue;

    /**
     * Создает новый счетчик событий
     */
    public EventCounter() {
        this.minuteEventQueue = new LinkedBlockingQueue<>();
        this.hourEventQueue = new LinkedBlockingQueue<>();
        this.dayEventQueue = new LinkedBlockingQueue<>();

        this.minuteEventCounter = new AtomicInteger();
        this.hourEventCounter = new AtomicInteger();
        this.dayEventCounter = new AtomicInteger();

        this.executorService = Executors.newFixedThreadPool(3);

        final QueueConsumer minuteConsumer = new QueueConsumer(
                minuteEventQueue,
                minuteEventCounter,
                hourEventQueue,
                hourEventCounter,
                HOUR_OFFSET_IN_MILLIS);
        final QueueConsumer hourConsumer = new QueueConsumer(
                hourEventQueue,
                hourEventCounter,
                dayEventQueue,
                dayEventCounter,
                DAY_OFFSET_IN_MILLIS);
        final QueueConsumer dayConsumer = new QueueConsumer(
                dayEventQueue,
                dayEventCounter);

        executorService.submit(minuteConsumer);
        executorService.submit(hourConsumer);
        executorService.submit(dayConsumer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean registerEvent() {
        //Пытаемся зарегистрировать событие
        //Помещаем в очередь его время жизни
        final boolean isRegistered = minuteEventQueue.offer(
                System.currentTimeMillis() + MINUTE_OFFSET_IN_MILLIS);
        //Если удачно, увеличиваием счетчик событий
        if (isRegistered) {
            minuteEventCounter.incrementAndGet();
        }
        return isRegistered;
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
}
