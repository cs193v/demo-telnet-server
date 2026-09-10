import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Listens on port 6789 and prints one line of text per incoming connection.
 *
 * Run with: java TelnetServer.java
 */
public final class TelnetServer {
    private static final int PORT = 6789;
    private static final int BACKLOG = 50;

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
    private static final String RESET = "\033[0m";
    private static final String CLEAR_LINE = "\r\033[K";
    private static final String HIDE_CURSOR = "\033[?25l";
    private static final String SHOW_CURSOR = "\033[?25h";

    private static final PrintStream out =
            new PrintStream(new FileOutputStream(FileDescriptor.out), false, StandardCharsets.UTF_8);
    private static final boolean interactive =
            System.console() != null && System.console().isTerminal();

    /** Guards the cursor: the spinner and the message printer share one line of screen. */
    private static final Object console = new Object();
    private static final AtomicInteger liveConnections = new AtomicInteger();

    /** Shown in the spinner so people in the room know where to point telnet. */
    private static final String LISTEN_ADDRESS = primaryAddress();

    public static void main(String[] args) throws IOException {
        // Binding to an IPv4 wildcard keeps peer addresses dotted quads, so the column never grows.
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), PORT), BACKLOG);

        if (interactive) {
            Runtime.getRuntime().addShutdownHook(new Thread(TelnetServer::restoreTerminal));
            out.print(HIDE_CURSOR);
            out.flush();
            Thread spinner = new Thread(TelnetServer::spin, "spinner");
            spinner.setDaemon(true);
            spinner.start();
        }

        while (true) {
            Socket socket = server.accept();
            liveConnections.incrementAndGet();
            Thread worker = new Thread(() -> handle(socket), "connection");
            worker.setDaemon(true);
            worker.start();
        }
    }

    private static void handle(Socket socket) {
        try (socket) {
            Line line = readLine(socket.getInputStream());
            // A client that hangs up without typing anything has nothing to say.
            if (line.terminated() || !line.text().isEmpty()) {
                emit(socket.getInetAddress().getHostAddress(), sanitize(line.text()));
            }
        } catch (IOException e) {
            // The client vanished mid-line; there is nothing worth reporting.
        } finally {
            liveConnections.decrementAndGet();
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

        while ((b = in.read()) != -1) {
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
            String text = truncate(status(), columns() - 3);
            synchronized (console) {
                out.print(CLEAR_LINE + BOLD_BRIGHT_BLUE + FRAMES[frame] + RESET
                        + BRIGHT_WHITE + " " + text + RESET);
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

    private static String status() {
        int live = liveConnections.get();
        String where = " to " + LISTEN_ADDRESS + " on port " + PORT;
        if (live == 0) return "Waiting for a connection" + where + "...";
        return live + (live == 1 ? " live connection" : " live connections") + where
                + "; waiting for input...";
    }

    /**
     * The address on whichever interface the default route uses, which is the one a client
     * elsewhere on the network should telnet to. The listener itself is bound to the wildcard,
     * so this is advisory: connections to any local address are still accepted.
     */
    private static String primaryAddress() {
        try (DatagramSocket probe = new DatagramSocket()) {
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
