import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Prints one line of text per connection that relay.py, running on the host, passes in from the
 * room. Every connection arrives from loopback, so the relay starts each one with a PROXY
 * protocol v1 line saying who really made it.
 *
 * Run with: java TelnetServer.java (and python3 relay.py on the host)
 */
public final class TelnetServer {
    /** Where the relay connects. The host reaches it through the container's port forwarding. */
    private static final int RELAY_PORT = 6790;
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
    private static final String BOLD_BRIGHT_WHITE = "\033[1;97m";
    private static final String WHITE = "\033[37m";
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

    /** Where people in the room should point telnet, as the relay announces it; null until then. */
    private record Endpoint(String address, int port) {}
    private static final AtomicReference<Endpoint> publicEndpoint = new AtomicReference<>();

    public static void main(String[] args) throws IOException {
        // IPv4 loopback specifically: the host's forwarding reaches it, and nothing else can.
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), RELAY_PORT), BACKLOG);

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
            Thread worker = new Thread(() -> handle(socket), "connection");
            worker.setDaemon(true);
            worker.start();
        }
    }

    private static void handle(Socket socket) {
        try (socket) {
            InputStream in = socket.getInputStream();
            String[] header = readHeader(in).split(" ");
            if (header.length == 3 && header[0].equals("RELAY")) {
                holdAnnouncement(in, new Endpoint(header[1], Integer.parseInt(header[2])));
            } else if (header.length == 6 && header[0].equals("PROXY") && header[1].equals("TCP4")) {
                converse(in, header[2]);
            }
            // Anything else did not come from the relay, so there is no one to credit it to.
        } catch (IOException | NumberFormatException e) {
            // The client vanished mid-line, or the relay sent nonsense; nothing worth reporting.
        }
    }

    private static void converse(InputStream in, String address) throws IOException {
        liveConnections.incrementAndGet();
        try {
            Line line = readLine(in);
            // A client that hangs up without typing anything has nothing to say.
            if (line.terminated() || !line.text().isEmpty()) {
                emit(address, sanitize(line.text()));
            }
        } finally {
            liveConnections.decrementAndGet();
        }
    }

    /** The relay holds this connection open for as long as it is up, and sends nothing more. */
    private static void holdAnnouncement(InputStream in, Endpoint announced) throws IOException {
        publicEndpoint.set(announced);
        try {
            while (in.read() != -1) {}
        } finally {
            // Only forget it if a restarted relay has not already announced again.
            publicEndpoint.compareAndSet(announced, null);
        }
    }

    /** PROXY protocol v1 caps its line at 107 bytes; ours is the same shape or shorter. */
    private static final int MAX_HEADER = 107;

    /** The relay's first line, without its CRLF. */
    private static String readHeader(InputStream in) throws IOException {
        StringBuilder header = new StringBuilder();
        int b;
        while ((b = in.read()) != '\n') {
            if (b == -1 || header.length() == MAX_HEADER) throw new IOException("No header");
            if (b != '\r') header.append((char) b);
        }
        return header.toString();
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
        Endpoint endpoint = publicEndpoint.get();
        if (endpoint == null) return List.of(new Span(BRIGHT_WHITE, "Starting up..."));

        int live = liveConnections.get();
        // The singular's extra space keeps the command in the same column as the plural's.
        String count = live + (live == 1 ? " connection.  " : " connections. ");
        // The connective is dimmer, so the count and the command are what stand out.
        return List.of(new Span(BRIGHT_WHITE, count),
                new Span(WHITE, "Connect with this command: "),
                new Span(BOLD_BRIGHT_WHITE, "telnet " + endpoint.address() + " " + endpoint.port()));
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
