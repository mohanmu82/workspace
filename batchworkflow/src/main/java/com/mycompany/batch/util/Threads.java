package com.mycompany.batch.util;

/**
 * Fire-and-forget background threads (Java 17 target — no virtual threads available).
 * Threads are daemon so they never block JVM shutdown, matching prior virtual-thread behavior.
 */
public final class Threads {

    private Threads() {}

    public static Thread startDaemon(Runnable task) {
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
        return t;
    }

    public static Thread startDaemon(String name, Runnable task) {
        Thread t = new Thread(task, name);
        t.setDaemon(true);
        t.start();
        return t;
    }
}
