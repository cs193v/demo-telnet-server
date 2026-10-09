import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Listens on port 6789 at this machine's primary address and prints one line of text per
 * incoming connection.
 *
 * Run with: java TelnetServer.java
 */
public final class TelnetServer {
    private static final int PORT = 6789;
    private static final int BACKLOG = 128;

    /**
     * Shown in telnet the moment a connection arrives, before anyone has typed. The %s is the
     * client's own address, exactly as it will label their message on the screen.
     */
    private static final String GREETING =
            // A blank line first, to set it apart from telnet's own "Escape character is" line.
            "\r\n"
            + "You're connected! Type a one-line message and press Enter.\r\n"
            + "It will appear on the screen at the front of the room.\r\n"
            + "Look for your IP address, %s, at the start of the line.\r\n";

    /**
     * How often to look for a new address, so the server can be started before reaching the
     * lecture hall.
     */
    private static final long ADDRESS_CHECK_MS = 2_000;

    /**
     * Two each for a class of 300, and few enough per address that no one person can take them
     * all.
     */
    private static final int MAX_CONNECTIONS = 600;
    private static final int MAX_PER_ADDRESS = 4;
    /**
     * Long enough to sit through an explanation before typing; short enough to reclaim abandoned
     * slots.
     */
    private static final int IDLE_MS = 10 * 60 * 1000;

    /** Every address is padded to the width of a maximal dotted quad. */
    private static final int IP_WIDTH = "NNN.NNN.NNN.NNN".length();
    private static final String SEPARATOR = " says: ";

    /** People are typing by hand, so a kilobyte of pending input is already absurd. */
    private static final int MAX_PENDING = 1024;
    private static final int FALLBACK_COLUMNS = 80;
    private static final long SPINNER_PERIOD_MS = 100;

    private static final String[] FRAMES = {"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};

    private static final String BOLD_BRIGHT_BLUE = "\033[1;94m";
    private static final String BRIGHT_WHITE = "\033[97m";
    private static final String BOLD_BRIGHT_WHITE = "\033[1;97m";
    private static final String WHITE = "\033[37m";
    private static final String RESET = "\033[0m";
    private static final String CLEAR_LINE = "\r\033[K";
    private static final String HIDE_CURSOR = "\033[?25l";
    private static final String SHOW_CURSOR = "\033[?25h";

    private static final PrintStream out =
            new PrintStream(new FileOutputStream(FileDescriptor.out), false, StandardCharsets.UTF_8);
    /** On Java 21 and earlier, and again from 25, there is a console only on a terminal. */
    private static final boolean interactive = System.console() != null;

    /** Guards the cursor: the spinner and the message printer share one line of screen. */
    private static final Object console = new Object();
    private static final AtomicInteger liveConnections = new AtomicInteger();
    /** Open connections from each address. Guards itself, and liveConnections too. */
    private static final Map<String, Integer> connectionsFrom = new HashMap<>();

    /** Where the listener is bound, and so where the room is told to telnet; null while unbound. */
    private static volatile String listeningOn;
    /** Why the last attempt to listen failed; null once one succeeds. */
    private static volatile String listenFailure;

    public static void main(String[] args) throws InterruptedException {
        if (interactive) {
            Runtime.getRuntime().addShutdownHook(new Thread(TelnetServer::restoreTerminal));
            out.print(HIDE_CURSOR);
            out.flush();
            Thread spinner = new Thread(TelnetServer::spin, "spinner");
            spinner.setDaemon(true);
            spinner.start();
        }

        ServerSocket listener = null;
        while (true) {
            String address = primaryAddress();
            if (address != null && !address.equals(listeningOn)) {
                if (listener != null) {
                    listeningOn = null;
                    // Its acceptor sees it closed and stops; connections already open carry on.
                    close(listener);
                }
                listener = listen(address);
            }
            Thread.sleep(ADDRESS_CHECK_MS);
        }
    }

    /**
     * A listener on just the address the room is told to use, not every interface (VPNs too).
     * Binding to an IPv4 address keeps peer addresses dotted quads, so the column never grows.
     */
    private static ServerSocket listen(String address) {
        ServerSocket listener;
        try {
            listener = new ServerSocket(PORT, BACKLOG, InetAddress.getByName(address));
        } catch (IOException e) {
            listenFailure = "Cannot listen on " + address + " port " + PORT + ": " + e.getMessage()
                    + ". Retrying...";
            report(listenFailure);
            return null;
        }

        Thread acceptor = new Thread(() -> accept(listener), "acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
        listeningOn = address;
        listenFailure = null;
        report("The room should telnet to " + address + " " + PORT + ".");
        return listener;
    }

    /** Hands each connection to a thread of its own, until the listener is closed. */
    private static void accept(ServerSocket listener) {
        while (true) {
            Socket socket;
            try {
                socket = listener.accept();
            } catch (IOException e) {
                if (listener.isClosed()) return;
                // Out of descriptors, or a client that gave up while queued. Neither is fatal.
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                continue;
            }

            String address = socket.getInetAddress().getHostAddress();
            if (!admit(address)) {
                close(socket);
                continue;
            }
            try {
                Thread worker = new Thread(() -> handle(socket, address), "connection");
                worker.setDaemon(true);
                worker.start();
            } catch (OutOfMemoryError e) {
                // Out of threads. Turn this one away and keep serving everyone else.
                release(address);
                close(socket);
            }
        }
    }

    private static boolean admit(String address) {
        synchronized (connectionsFrom) {
            if (liveConnections.get() >= MAX_CONNECTIONS
                    || connectionsFrom.getOrDefault(address, 0) >= MAX_PER_ADDRESS) {
                return false;
            }
            connectionsFrom.merge(address, 1, Integer::sum);
            liveConnections.incrementAndGet();
            return true;
        }
    }

    private static void release(String address) {
        synchronized (connectionsFrom) {
            // Dropping the entry at zero keeps the map to the addresses connected right now.
            connectionsFrom.computeIfPresent(address, (a, open) -> open == 1 ? null : open - 1);
            liveConnections.decrementAndGet();
        }
    }

    private static void handle(Socket socket, String address) {
        try (socket) {
            socket.setSoTimeout(IDLE_MS);
            socket.getOutputStream().write(
                    GREETING.formatted(address).getBytes(StandardCharsets.US_ASCII));
            Line line = readLine(socket.getInputStream());
            // A client that hangs up without typing anything has nothing to say.
            if (line.terminated() || !line.text().isEmpty()) {
                emit(address, sanitize(line.text()));
            }
        } catch (IOException e) {
            // The client vanished mid-line; there is nothing worth reporting.
        } finally {
            release(address);
        }
    }

    private record Line(String text, boolean terminated) {}

    private static final int DATA = 0, AFTER_IAC = 1, AWAIT_OPTION = 2, SUBNEG = 3, SUBNEG_IAC = 4;
    private static final int IAC = 255, SB = 250, SE = 240, WILL = 251, DONT = 254;

    /**
     * Reads up to the first newline, discarding telnet negotiation and applying backspaces as
     * they arrive (which is equivalent to applying them to the whole raw string afterwards).
     */
    private static Line readLine(InputStream in) throws IOException {
        StringBuilder pending = new StringBuilder();
        int state = DATA;
        int b;

        while ((b = nextByte(in)) != -1) {
            switch (state) {
                case AFTER_IAC -> {
                    if (b >= WILL && b <= DONT) {
                        state = AWAIT_OPTION;
                    } else if (b == SB) {
                        state = SUBNEG;
                    } else {
                        // A lone command byte, or an escaped 0xFF that the ASCII filter would drop.
                        state = DATA;
                    }
                }
                case AWAIT_OPTION -> state = DATA;
                case SUBNEG -> { if (b == IAC) state = SUBNEG_IAC; }
                case SUBNEG_IAC -> state = (b == SE) ? DATA : SUBNEG;
                default -> {
                    if (b == IAC) {
                        state = AFTER_IAC;
                    } else if (b == '\n') {
                        return new Line(pending.toString(), true);
                    } else if (b == 8 || b == 127) {
                        if (pending.length() > 0) pending.deleteCharAt(pending.length() - 1);
                    } else {
                        pending.append((char) b);
                        if (pending.length() > MAX_PENDING) pending.deleteCharAt(0);
                    }
                }
            }
        }
        return new Line(pending.toString(), false);
    }

    /**
     * The next byte, or -1 once the client hangs up or goes quiet. Treating the two alike means a
     * line typed without Enter still counts either way.
     */
    private static int nextByte(InputStream in) throws IOException {
        try {
            return in.read();
        } catch (SocketTimeoutException e) {
            return -1;
        }
    }

    /** Keeps the printable ASCII range: letters, digits, punctuation and the space. */
    private static String sanitize(String raw) {
        StringBuilder clean = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c >= 0x20 && c <= 0x7E) clean.append(c);
        }
        return clean.toString();
    }

    private static void emit(String address, String message) {
        String paddedAddress = pad(address);
        // One column of slack keeps a full-width line from wrapping on its own newline.
        int room = columns() - 1 - paddedAddress.length() - SEPARATOR.length();
        String shown = truncate(message, room);

        synchronized (console) {
            if (interactive) {
                out.print(CLEAR_LINE);
                out.println(BOLD_BRIGHT_BLUE + paddedAddress + RESET
                        + BRIGHT_WHITE + SEPARATOR + shown + RESET);
            } else {
                out.println(paddedAddress + SEPARATOR + shown);
            }
            out.flush();
        }
    }

    private static String pad(String address) {
        if (address.length() >= IP_WIDTH) return address;
        return address + " ".repeat(IP_WIDTH - address.length());
    }

    private static String truncate(String message, int room) {
        if (room <= 0) return "";
        if (message.length() <= room) return message;
        return message.substring(0, room - 1) + "…";
    }

    private static void spin() {
        int frame = 0;
        while (true) {
            // Budget: one column of slack, plus the spinner glyph and its trailing space.
            String text = render(status(), columns() - 3);
            synchronized (console) {
                out.print(CLEAR_LINE + BOLD_BRIGHT_BLUE + FRAMES[frame] + RESET + " " + text);
                out.flush();
            }
            frame = (frame + 1) % FRAMES.length;
            try {
                Thread.sleep(SPINNER_PERIOD_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** A run of the spinner's text, and the escape code it is drawn in. */
    private record Span(String style, String text) {}

    private static List<Span> status() {
        String address = listeningOn;
        if (address == null) {
            String failure = listenFailure;
            return List.of(new Span(BRIGHT_WHITE, failure != null ? failure : "Starting up..."));
        }

        int live = liveConnections.get();
        // The singular's extra space keeps the command in the same column as the plural's.
        String count = live + (live == 1 ? " connection.  " : " connections. ");
        // The connective is dimmer, so the count and the command are what stand out.
        return List.of(new Span(BRIGHT_WHITE, count),
                new Span(WHITE, "Connect with this command: "),
                new Span(BOLD_BRIGHT_WHITE, "telnet " + address + " " + PORT));
    }

    /** The spans in their styles, truncated to fit in room columns. */
    private static String render(List<Span> spans, int room) {
        StringBuilder plain = new StringBuilder();
        for (Span span : spans) plain.append(span.text());
        // Styled by position after truncating, so escape codes never count against the width.
        String shown = truncate(plain.toString(), room);

        StringBuilder styled = new StringBuilder();
        int start = 0;
        for (Span span : spans) {
            int end = Math.min(shown.length(), start + span.text().length());
            if (start < end) styled.append(span.style()).append(shown, start, end).append(RESET);
            start = end;
        }
        return styled.toString();
    }

    /** Only the main thread reports, so this needs no guarding. */
    private static String lastReport;

    /** With no spinner to show it on, news of the listener goes to stderr, once per change. */
    private static void report(String message) {
        if (!interactive && !message.equals(lastReport)) System.err.println(message);
        lastReport = message;
    }

    /**
     * The address on whichever interface the default route uses, which is the one a client
     * elsewhere on the network should telnet to, or null if there was not even a descriptor to
     * spare for finding out.
     */
    private static String primaryAddress() {
        DatagramSocket probe;
        try {
            probe = new DatagramSocket();
        } catch (SocketException e) {
            return null;
        }
        try (probe) {
            // Connecting a UDP socket sends no packets; it only selects the outbound interface.
            probe.connect(InetAddress.getByName("8.8.8.8"), 53);
            InetAddress local = probe.getLocalAddress();
            if (local instanceof Inet4Address && !local.isAnyLocalAddress()) {
                return local.getHostAddress();
            }
        } catch (Exception e) {
            // No route off this machine; loopback is the only address that can be reached.
        }
        return "127.0.0.1";
    }

    private static void close(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // Closed as far as anyone can tell; there is nothing more to do.
        }
    }

    private static volatile int cachedColumns = FALLBACK_COLUMNS;
    private static volatile long columnsCheckedAt = 0;

    /** Terminal width, re-read at most once a second so a mid-demo resize is picked up. */
    private static int columns() {
        if (!interactive) return FALLBACK_COLUMNS;
        long now = System.nanoTime();
        if (columnsCheckedAt != 0 && now - columnsCheckedAt < 1_000_000_000L) return cachedColumns;
        columnsCheckedAt = now;
        cachedColumns = queryColumns();
        return cachedColumns;
    }

    private static int queryColumns() {
        try {
            Process stty = new ProcessBuilder("sh", "-c", "stty size < /dev/tty 2>/dev/null").start();
            String size;
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stty.getInputStream(), StandardCharsets.UTF_8))) {
                size = reader.readLine();
            }
            stty.waitFor();
            if (size != null) {
                String[] parts = size.trim().split("\\s+");
                if (parts.length == 2) {
                    int cols = Integer.parseInt(parts[1]);
                    if (cols > 0) return cols;
                }
            }
        } catch (Exception e) {
            // Fall through to the default width.
        }
        return FALLBACK_COLUMNS;
    }

    private static void restoreTerminal() {
        synchronized (console) {
            out.print(CLEAR_LINE + SHOW_CURSOR);
            out.flush();
        }
    }

    private TelnetServer() {}
}
