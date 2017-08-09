package statsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * StatsD Client
 */
public final class StatsDClient {

    private final String prefix;
    private final InetSocketAddress clientSocket;
    private final DatagramChannel clientChannel;

    /**
     * https://github.com/etsy/statsd/blob/master/docs/metric_types.md
     *
     * Be careful to keep the total length of the payload within your network's MTU.
     * There is no single good value to use, but here are some guidelines for common network scenarios:
     * Fast Ethernet (1432) - This is most likely for Intranets.
     * Gigabit Ethernet (8932) - Jumbo frames can make use of this feature much more efficient.
     * Commodity Internet (512) - If you are routing over the internet a value in this range will be reasonable.
     * You might be able to go higher, but you are at the mercy of all the hops in your route.
     * (These payload numbers take into account the maximum IP + UDP header sizes)
     *
     * Finding the largest MTU:
     * (Linux):   ping  -M do -s 1472 { StatsD hostname }
     * (Mac OSX): ping -D -s 1472 { StatsD hostname }
     */
    private static final int MAXIMUM_TRANSFER_UNIT = 1472;
    private final ByteBuffer buffer;
    private boolean buffering = false;

    private final static Random random = new Random(19580427);

    /* Executor termination timeout in seconds */
    private static final int TERMINATION_TIMEOUT = 30;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Constructor
     *
     * @param prefix prefix name for you metrics
     * @param hostname
     * @param port
     * @throws IOException
     */
    public StatsDClient(final String prefix, final String hostname, int port) throws IOException {
        this(prefix, hostname, port, false);
    }

    /**
     * Constructor
     *
     * @param prefix
     * @param hostname
     * @param port
     * @param buffering
     * @throws IOException
     */
    public StatsDClient(final String prefix, final String hostname, int port, boolean buffering) throws IOException {
        this.prefix = prefix;

        this.clientSocket = new InetSocketAddress(hostname, port);
        this.clientChannel = DatagramChannel.open();

        this.buffering = buffering;
        this.buffer = ByteBuffer.allocate(MAXIMUM_TRANSFER_UNIT);
    }

    /**
     * Shutdowns internal thread executor,
     * closes UDP socket channel and clears data buffer.
     */
    public void shutdown() {
        try {
            executor.shutdown();
            executor.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS);
        }
        catch (Exception e) { e.printStackTrace(); }

        try {
            if (buffer != null) flush();
        }
        catch (Exception e) { e.printStackTrace(); }

        try {
            if (clientChannel != null) clientChannel.close();
        }
        catch (Exception e) { e.printStackTrace(); }
        finally {
            if (buffer != null) {
                buffer.clear();
            }
        }
    }

    /**
     * Sends counter
     *
     * @param key
     * @param delta
     * @param sampleRate
     */
    public void count(final String key, int delta, double sampleRate) {
        send(String.format("%s.%s:%d|c", prefix, key, delta), sampleRate);
    }

    /**
     * Sends counter with delta = 1
     *
     * @param key
     * @param sampleRate
     */
    public void inc(final String key, double sampleRate) {
        count(key, 1, sampleRate);
    }

    /**
     * Sends counter with delta = -1
     *
     * @param key
     * @param sampleRate
     */
    public void dec(final String key, double sampleRate) {
        count(key, -1, sampleRate);
    }

    /**
     * Sends timer
     *
     * @param key
     * @param timeInMs
     * @param sampleRate
     */
    public void time(final String key, long timeInMs, double sampleRate) {
        send(String.format("%s.%s:%d|ms", prefix, key, timeInMs), sampleRate);
    }

    /**
     * Sends gauge
     *
     * @param key
     * @param value
     * @param sampleRate
     */
    public void gauge(final String key, double value, double sampleRate) {
        send(String.format("%s.%s:%.3f|g", prefix, key, value), sampleRate);
    }

    /**
     * StatsD supports counting unique occurences of events between flushes,
     * using a Set to store all occuring events.
     *
     * @param key
     * @param value
     * @param sampleRate
     */
    public void set(final String key, final String value, double sampleRate) {
        send(String.format("%s.%s:%s|s", prefix, key, value), sampleRate);
    }

    private void send(final String message, final double sampleRate) {
        // sampling ?
        final boolean doSampling = sampleRate > 0.0 && sampleRate < 1.0;
        if (doSampling && random.nextDouble() > sampleRate) return;

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    final byte[] data = doSampling ? String.format("%s|@%f", message, sampleRate).getBytes() : message.getBytes();
                    if (buffer.remaining() < (1 + data.length)) flush();
                    if (buffer.position() > 0) buffer.put((byte) '\n');

                    buffer.put(data);
                    if (!buffering) flush();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        });
    }

    private void flush() throws IOException {
        final int pos = buffer.position();

        // check if buffer is not empty
        if (pos > 0) {
            // send
            buffer.flip();
            clientChannel.send(buffer, clientSocket);

            // reset the buffer
            buffer.limit(buffer.capacity());
            buffer.rewind();
        }
    }
}