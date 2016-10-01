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
    /**
     * Планировщик для выполнения отложенных задач.
     */
    private final ScheduledExecutorService executorService;
    /**
     * Счетчик количества событий которые ваполнялись меньше минуты назад.
     */
    private final AtomicInteger minuteEventCounter = new AtomicInteger();
    /**
     * Счетчик количества событий которые выполнялись больше минуты,
     * но меньше часа назад.
     */
    private final AtomicInteger hourEventCounter = new AtomicInteger();
    /**
     * Счетчик количества событий которые выполнялись больше часа,
     * но меньше суток назад.
     */
    private final AtomicInteger dayEventCounter = new AtomicInteger();

    /**
     * Создает новый счетчик событий
     */
    public EventCounter() {
        final int processors = Runtime.getRuntime().availableProcessors();
        executorService = Executors.newScheduledThreadPool(processors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean registerEvent() {
        final CounterUpdateTask counterUpdateTask = new CounterUpdateTask(MINUTES);
        minuteEventCounter.incrementAndGet();
        try {
            //Откладываем выполнение задачи на одну минуту
            executorService.schedule(counterUpdateTask, 1, MINUTES);
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

    /**
     * Задача обновляющая счетчики задач за минуту, час и день.
     */
    private class CounterUpdateTask implements Runnable {
        /**
         * Для указания текущего типа счетчика для обновления.
         */
        private final TimeUnit timeUnit;

        /**
         * Создает новую задачу обновления счетчиков
         *
         * @param timeUnit тип текущего счетчика для обновления.
         */
        CounterUpdateTask(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public void run() {
            final CounterUpdateTask nextCounterUpdateTask;
            //Проверяем какие счетчики нужно обновить
            switch (timeUnit) {
                case MINUTES:
                    nextCounterUpdateTask = new CounterUpdateTask(HOURS);
                    //Отмечаем что событие произошло больше минуты назад
                    minuteEventCounter.decrementAndGet();
                    //Отмечаем что событие произошло меньше часа назад
                    hourEventCounter.incrementAndGet();
                    try {
                        //Откладываем задачу на 59 минут т.к. текущая задача
                        //выполняется через минуту после события.
                        //Новая задача будет выполнена через час после события.
                        executorService.schedule(nextCounterUpdateTask, 59, MINUTES);
                    } catch (RejectedExecutionException ex) {
                        hourEventCounter.decrementAndGet();
                    }
                    break;
                case HOURS:
                    nextCounterUpdateTask = new CounterUpdateTask(DAYS);
                    //Отмечаем что событие произошло больше часа назад
                    hourEventCounter.decrementAndGet();
                    //Отмечаем что событие произошло меньше суток назад
                    dayEventCounter.incrementAndGet();
                    try {
                        //Откладываем задачу на 23 часа т.к. текущая задача
                        //выполняется через час после события.
                        //Новая задача будет выполнена через сутки после события.
                        executorService.schedule(nextCounterUpdateTask, 23, HOURS);
                    } catch (RejectedExecutionException ex) {
                        dayEventCounter.decrementAndGet();
                    }
                    break;
                case DAYS:
                    //Отмечаем что событие произошло больше суток назад
                    dayEventCounter.decrementAndGet();
            }
        }
    }
}
