package com.ym.event_counter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Класс предназначеный для очистки входной очереди от событий
 * время жизни которых истекло.
 * Удаленные события могут быть помещены в выходную очередь
 * с заданным временем жизни.
 *
 * @author Dmitry
 * @since 08.10.2016
 */
final class QueueConsumer implements Runnable {
    /**
     * Входящая очередь событий.
     */
    private final BlockingQueue<Long> inputQueue;
    /**
     * Исходящая очередь событий.
     */
    private final BlockingQueue<Long> outputQueue;
    /**
     * Счетчик событий во входной очереди
     */
    private final AtomicInteger inputQueueCounter;
    /**
     * Счетчик событий в исходящей очереди
     */
    private final AtomicInteger outputQueueCounter;
    /**
     * На сколько увелится время жизни события при
     * перемещении в исходящую очередь
     */
    private final long offset;

    /**
     * Создает объект предназначеный для очистки входной очереди от событий
     * время жизни которых меньше текущего времеми.
     * Если выходная очередь и счетчик событий в исходящей очереди заданы,
     * то удаленные события помещаются в выходную очередь.
     * При этом время их жизни увеличится на величину смещения.
     *
     * @param inputQueue         входная очередь от событий
     * @param inputQueueCounter  счетчик событий во входной очереди
     * @param outputQueue        исходящая очередь от событий
     * @param outputQueueCounter счетчик событий в выходной очереди
     * @param offset             Время (в миллисекундах) на которое следует продлить
     *                           жизнь события при перемещении в выходную очередь
     */
    QueueConsumer(
            final BlockingQueue<Long> inputQueue,
            final AtomicInteger inputQueueCounter,
            final BlockingQueue<Long> outputQueue,
            final AtomicInteger outputQueueCounter,
            final long offset) {
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.offset = offset;
        this.inputQueueCounter = inputQueueCounter;
        this.outputQueueCounter = outputQueueCounter;
    }

    /**
     * Создает объект предназначеный для очистки входной очереди от событий
     * время жизни которых меньше текущего времеми.
     *
     * @param inputQueue        входная очередь от событий
     * @param inputQueueCounter счетчик событий во входной очереди
     */
    QueueConsumer(
            final BlockingQueue<Long> inputQueue,
            final AtomicInteger inputQueueCounter) {
        this(inputQueue, inputQueueCounter, null, null, 0);
    }

    @Override
    public void run() {
        //Событие для проверки
        Long currentEvent = null;
        while (true) {
            if (currentEvent == null) {
                try {
                    //Пытаемся получить событие.
                    //Если очередь пуста, ждем пока не придет событие
                    currentEvent = inputQueue.take();
                } catch (InterruptedException e) {
                    //Завершаем выполнение работы с очередью
                    break;
                }
            }
            //Проверяем время жизни события
            if (System.currentTimeMillis() >= currentEvent) {
                //Если оно уже истекло, уменьшаем количество событий
                inputQueueCounter.decrementAndGet();
                //Если выходная очередь и счетчик событий в выходной очереди заданы
                if (outputQueue != null && outputQueueCounter != null) {
                    //перекладываем событие в выходную очередь
                    //увеличив время жизни на заданное смещение
                    outputQueueCounter.incrementAndGet();
                    outputQueue.add(currentEvent + offset);
                }
                currentEvent = null;
            }
        }
    }
}
