import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-threaded Message Queue — Priority-aware
 * - Topics use a PriorityQueue: higher priority first, then shorter duration, then FIFO by arrival time.
 * - Producers & Consumers using Locks + Conditions (notEmpty, notFull)
 * - bounded buffer, TTL + GC thread
 * Thread-safe queue: Topic uses one ReentrantLock with notEmpty/notFull conditions. Correct locking/unlocking and signaling.
 * Bounded buffer: producers block on notFull, consumers signal()/signalAll() when space frees.
 * TTL + GC: periodic GcWorker removes expired messages; consumer also skips head-expired messages — solid.
 * Graceful shutdown: Broker.stop() flips running and signalAll()s both conditions; consumers check running after wakes.
 * Live dashboard: shows per-topic sizes, head message info, distribution by topic/consumer, and per-priority averages.
 * Concurrent stats: ConcurrentHashMap + AtomicLong avoid contention and racey increments.
 */

public class message_broker {

    static final int NUM_TOPICS = 3;
    static final int NUM_PRODUCERS = 4;
    static final int NUM_CONSUMERS = 3;
    static final long RUN_SECONDS = 15;         // set 0 to run indefinitely

    static final long DASHBOARD_PERIOD_MS = 2_000;

    static final boolean ENABLE_BOUNDED_BUFFER = true;
    static final int TOPIC_CAPACITY = 16;       // only used if ENABLE_BOUNDED_BUFFER

    static final boolean ENABLE_TTL = true;
    static final long DEFAULT_TTL_MS = 3_000;   // message expires after 3 seconds
    static final long GC_PERIOD_MS = 500;       // GC scan period

    static final class Message {
        final long id;
        final String payload;
        final int priority;      // 0..4 (higher = more important)
        final int durationMs;    // simulated processing time
        final long createdAtMs;
        final long ttlMs;        // if ENABLE_TTL: time-to-live

        Message(long id, String payload, int priority, int durationMs, long ttlMs) {
            this.id = id;
            this.payload = payload;
            this.priority = priority;
            this.durationMs = durationMs;
            this.createdAtMs = System.currentTimeMillis();
            this.ttlMs = ttlMs;
        }

        boolean isExpired(long now) {
            if (!ENABLE_TTL) return false;
            return (now - createdAtMs) >= ttlMs;
        }
    }

    /** Thread-safe PRIORITY queue with optional capacity + TTL helpers. */
    static final class Topic {
        final String name;

        // Comparator: higher priority first, then shorter duration, then older first (FIFO among equals)
        private static final Comparator<Message> MSG_CMP = (a, b) -> {
            if (a.priority != b.priority) return Integer.compare(b.priority, a.priority);
            if (a.durationMs != b.durationMs) return Integer.compare(a.durationMs, b.durationMs);
            return Long.compare(a.createdAtMs, b.createdAtMs);
        };

        final PriorityQueue<Message> q = new PriorityQueue<>(MSG_CMP);

        final ReentrantLock lock = new ReentrantLock();
        final Condition notEmpty = lock.newCondition();  // for consumers
        final Condition notFull  = lock.newCondition();  // for producers
        final int capacity; // if <= 0 => unbounded

        Topic(String name, int capacity) {
            this.name = name;
            this.capacity = ENABLE_BOUNDED_BUFFER ? capacity : -1;
        }

        /** Enqueue; blocks if bounded & full. */
        void put(Message m) throws InterruptedException {
            lock.lock();
            try {
                while (isFull()) {
                    notFull.await();
                }
                q.add(m);
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        /**
         * Cooperative blocking take:
         * waits while empty; skips expired heads; returns null if stop requested.
         */
        Message take(AtomicBoolean runningFlag) throws InterruptedException {
            lock.lock();
            try {
                while (q.isEmpty()) {
                    if (!runningFlag.get()) return null;
                    notEmpty.await(200, TimeUnit.MILLISECONDS);
                }
                // Skip expired messages at head
                while (!q.isEmpty()) {
                    Message head = q.peek();
                    if (ENABLE_TTL && head.isExpired(System.currentTimeMillis())) {
                        q.poll(); // drop expired
                        if (ENABLE_BOUNDED_BUFFER) notFull.signalAll();
                        if (!runningFlag.get() && q.isEmpty()) return null;
                        continue;
                    }
                    break;
                }
                if (q.isEmpty()) return null; // after skipping expired, might be empty
                Message m = q.poll();
                if (ENABLE_BOUNDED_BUFFER) notFull.signal();  // wakeup producer
                return m;
            } finally {
                lock.unlock();
            }
        }

        /** Remove expired messages; return count removed. */
        int removeExpired() {
            if (!ENABLE_TTL) return 0;
            int removed = 0;
            long now = System.currentTimeMillis();
            lock.lock();
            try {
                for (Iterator<Message> it = q.iterator(); it.hasNext();) {
                    Message m = it.next();
                    if (m.isExpired(now)) {
                        it.remove();
                        removed++;
                    }
                }
                if (removed > 0 && ENABLE_BOUNDED_BUFFER) {
                    notFull.signalAll();
                }
            } finally {
                lock.unlock();
            }
            return removed;
        }

        /** Size (approximate; guarded by lock for correctness). */
        int size() {
            lock.lock();
            try { return q.size(); }
            finally { lock.unlock(); }
        }

        boolean isFull() {
            return ENABLE_BOUNDED_BUFFER && capacity > 0 && q.size() >= capacity;
        }

        /** Peek current best (non-destructive). May return an expired head if GC hasn't cleaned yet. */
        Message peekHead() {
            lock.lock();
            try {
                // ensure we don't show an expired head in the dashboard
                while (!q.isEmpty()) {
                    Message h = q.peek();
                    if (ENABLE_TTL && h.isExpired(System.currentTimeMillis())) {
                        q.poll(); // drop it lazily
                        if (ENABLE_BOUNDED_BUFFER) notFull.signalAll();
                        continue;
                    }
                    return h;
                }
                return null;
            } finally {
                lock.unlock();
            }
        }
    }

    static final class Broker {
        final Topic[] topics;
        final AtomicBoolean running = new AtomicBoolean(true);

        final AtomicLong processedCount = new AtomicLong(0);
        final Map<Integer, AtomicLong> perPriorityCount = new HashMap<>();
        final Map<Integer, AtomicLong> perPriorityTotalDurationMs = new HashMap<>();

        final Map<String, AtomicLong> processedByTopic = new ConcurrentHashMap<>();
        final Map<Integer, AtomicLong> processedByConsumer = new ConcurrentHashMap<>();

        Broker(int numTopics) {
            this.topics = new Topic[numTopics];
            for (int i = 0; i < numTopics; i++) {
                topics[i] = new Topic("topic-" + i, TOPIC_CAPACITY);
            }
            for (int p = 0; p <= 4; p++) {
                perPriorityCount.put(p, new AtomicLong(0));
                perPriorityTotalDurationMs.put(p, new AtomicLong(0));
            }
            for (Topic t : topics) {
                processedByTopic.put(t.name, new AtomicLong(0));
            }
            for (int cid = 0; cid < NUM_CONSUMERS; cid++) {
                processedByConsumer.put(cid, new AtomicLong(0));
            }
        }

        void stop() {
            running.set(false);
            for (Topic t : topics) {
                t.lock.lock();
                try {
                    t.notEmpty.signalAll();
                    t.notFull.signalAll();
                } finally {
                    t.lock.unlock();
                }
            }
        }

        void recordProcessed(Message m, String topicName, int consumerId) {
            processedCount.incrementAndGet();
            perPriorityCount.get(m.priority).incrementAndGet();
            perPriorityTotalDurationMs.get(m.priority).addAndGet(m.durationMs);

            processedByTopic.get(topicName).incrementAndGet();
            processedByConsumer.get(consumerId).incrementAndGet();
        }

        double avgDurationForPriority(int p) {
            long c = perPriorityCount.get(p).get();
            if (c == 0) return 0.0;
            return perPriorityTotalDurationMs.get(p).get() / (double) c;
        }
    }

    static final class Producer implements Runnable {
        final int id;
        final Broker broker;
        final Random rnd = new Random();

        Producer(int id, Broker broker) {
            this.id = id;
            this.broker = broker;
        }

        @Override public void run() {
            try {
                while (broker.running.get()) {
                    Topic t = broker.topics[rnd.nextInt(broker.topics.length)];
                    long msgId = Math.abs(rnd.nextLong()) % 100_000;
                    int pri = rnd.nextInt(5); // 0..4
                    int dur = 100 + rnd.nextInt(400);
                    long ttl = ENABLE_TTL ? DEFAULT_TTL_MS : 0;
                    Message m = new Message(msgId, "Msg-" + msgId + " from P" + id, pri, dur, ttl);
                    t.put(m);
                    Thread.sleep(100 + rnd.nextInt(500));
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static final class Consumer implements Runnable {
        final int id;
        final Broker broker;
        final Random rnd = new Random();

        Consumer(int id, Broker broker) {
            this.id = id;
            this.broker = broker;
        }

        @Override public void run() {
            try {
                while (broker.running.get()) {
                    // pick most loaded topic to help balance
                    Topic t = pickTopic();
                    if (t == null) t = broker.topics[rnd.nextInt(broker.topics.length)];

                    // use priority-aware take()
                    Message m = t.take(broker.running);
                    if (m == null) continue; // stop requested or queue emptied after skipping expired

                    // process
                    Thread.sleep(m.durationMs);
                    broker.recordProcessed(m, t.name, id);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        private Topic pickTopic() {
            Topic best = null;
            int max = 0;
            for (Topic t : broker.topics) {
                int sz = t.size();
                if (sz > max) { max = sz; best = t; }
            }
            return (max > 0) ? best : null;
        }
    }

    /*  GARBAGE COLLECTOR & TTL(Time To Live)  */
    static final class GcWorker implements Runnable {
        final Broker broker;
        GcWorker(Broker broker) { this.broker = broker; }

        @Override public void run() {
            try {
                while (broker.running.get() && ENABLE_TTL) {
                    for (Topic t : broker.topics) {
                        t.removeExpired();
                        //System.out.println("removed");
                    }
                    Thread.sleep(GC_PERIOD_MS);
                }
            } catch (InterruptedException ignored) {}
        }
    }

    static final class Dashboard implements Runnable {
        final Broker broker;
        Dashboard(Broker b){ this.broker=b; }

        @Override public void run() {
            try {
                while (broker.running.get()) {
                    System.out.println();
                    System.out.println("========== DASHBOARD ==========");
                    for (Topic t : broker.topics) {
                        System.out.printf("  %-8s size=%d%s%n",
                                t.name, t.size(),
                                ENABLE_BOUNDED_BUFFER ? (" / cap=" + t.capacity) : "");
                        Message h = t.peekHead();
                        if (h != null) {
                            long age = System.currentTimeMillis() - h.createdAtMs;
                            System.out.printf("     head id=%d pri=%d dur=%dms age=%dms%n",
                                    h.id, h.priority, h.durationMs, age);
                        }
                    }

                    System.out.println("  -- distribution by topic --");
                    long tot = broker.processedCount.get();
                    for (Map.Entry<String, AtomicLong> e : broker.processedByTopic.entrySet()) {
                        long c = e.getValue().get();
                        double pct = (tot==0)?0.0:(100.0*c/tot);
                        System.out.printf("    %s: %d (%.1f%%)%n", e.getKey(), c, pct);
                    }

                    System.out.println("  -- distribution by consumer --");
                    for (int cid = 0; cid < NUM_CONSUMERS; cid++) {
                        long c = broker.processedByConsumer.get(cid).get();
                        double pct = (tot==0)?0.0:(100.0*c/tot);
                        System.out.printf("    C%-2d: %d (%.1f%%)%n", cid, c, pct);
                    }

                    System.out.println("  -- avg duration by priority --");
                    for (int p=0; p<=4; p++) {
                        System.out.printf("    pri=%d avg=%.1f ms%n", p, broker.avgDurationForPriority(p));
                    }
                    System.out.println("================================");
                    Thread.sleep(DASHBOARD_PERIOD_MS);
                }
            } catch (InterruptedException ignored) {}
        }
    }

    public static void main(String[] args) throws Exception {
        Broker broker = new Broker(NUM_TOPICS);

        // dashboard
        Thread dash = new Thread(new Dashboard(broker), "dashboard");
        dash.start();

        // producers
        List<Thread> producers = new ArrayList<>();
        for (int i = 0; i < NUM_PRODUCERS; i++) {
            Thread t = new Thread(new Producer(i, broker), "producer-" + i);
            t.start();
            producers.add(t);
        }

        // consumers
        List<Thread> consumers = new ArrayList<>();
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            Thread t = new Thread(new Consumer(i, broker), "consumer-" + i);
            t.start();
            consumers.add(t);
        }

        // GC
        Thread gc = null;
        if (ENABLE_TTL) {
            gc = new Thread(new GcWorker(broker), "gc");
            gc.start();
        }

        // run & stop
        if (RUN_SECONDS > 0) {
            sleepSec(RUN_SECONDS);
            broker.stop();
        } else {
            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));
        }

        // cleanup
        for (Thread t : producers) t.join();
        for (Thread t : consumers) t.join();
        dash.join();
        if (gc != null) gc.join();

        // summary
        System.out.println();
        System.out.println("===== SUMMARY =====");
        System.out.println("Processed total = " + broker.processedCount.get());
        for (int p = 0; p <= 4; p++) {
            System.out.printf("  pri=%d avg_dur=%.1f ms (count=%d)%n",
                    p, broker.avgDurationForPriority(p), broker.perPriorityCount.get(p).get());
        }
        System.out.println("===================");
        System.out.println("System shut down.");
    }

    private static void sleepSec(long s) {
        try { Thread.sleep(TimeUnit.SECONDS.toMillis(s)); }
        catch (InterruptedException ignored) {}
    }
}