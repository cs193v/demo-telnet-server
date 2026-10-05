#!/usr/bin/env python3
"""
Runs on the host. Accepts telnet connections from the room on port 6789 and relays each one to
TelnetServer in the container, which only the host's own loopback can reach.

The container sees every relayed connection arrive from its loopback, so each one starts with a
PROXY protocol v1 line naming the address it really came from. One extra connection, held open
for as long as both sides are up, tells the server which address the room should telnet to.

Traffic only flows one way. Nothing the room sends is interpreted here, and nothing from the
container is ever sent back: TelnetServer never writes, so anything that arrives from the
container ends the connection instead of reaching the room. Whatever happens to be listening on
the backend port, the room cannot read from it. The one thing the room does receive is GREETING,
which is fixed in this file.

Run with: python3 relay.py
"""

import collections
import resource
import select
import socket
import threading
import time

PUBLIC_PORT = 6789
BACKEND = ("127.0.0.1", 6790)
BACKLOG = 128
RETRY_SECONDS = 1

# Shown in telnet the moment a connection reaches the server, before anyone has typed. Plain ASCII
# lines, each ending in CRLF as telnet expects.
GREETING = (
    # A blank line first, to set it apart from telnet's own "Escape character is" line.
    "\r\n"
    "You're connected! Type a one-line message and press Enter.\r\n"
    "It will appear on the screen at the front of the room.\r\n"
)

# How often to look for a new address, so the relay can be started before reaching the lecture hall.
ADDRESS_CHECK_SECONDS = 2

# Two each for a class of 300, and few enough per address that no one person can take them all.
MAX_CONNECTIONS = 600
MAX_PER_ADDRESS = 4
# Long enough to sit through an explanation before typing; short enough to reclaim abandoned slots.
IDLE_SECONDS = 10 * 60
# Each connection holds two descriptors, the room's and the container's. These are for the rest.
SPARE_DESCRIPTORS = 32

# Where the listener is bound, which is also where the server tells the room to telnet. None while
# there is no listener.
listening_on = None

open_connections = collections.Counter()
open_connections_lock = threading.Lock()


def primary_address():
    """
    The address on whichever interface the default route uses, as TelnetServer once did, or None
    if there was not even a descriptor to spare for finding out.
    """
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        return None
    with probe:
        try:
            # Connecting a UDP socket sends no packets; it only selects the outbound interface.
            probe.connect(("8.8.8.8", 53))
            return probe.getsockname()[0]
        except OSError:
            # No route off this machine; loopback is the only address that can be reached.
            return "127.0.0.1"


def connection_budget():
    """How many connections fit in the descriptor limit, after raising it as far as allowed."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY:
        return MAX_CONNECTIONS
    wanted = 2 * MAX_CONNECTIONS + SPARE_DESCRIPTORS
    if soft < wanted:
        soft = wanted if hard == resource.RLIM_INFINITY else min(wanted, hard)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
    return min(MAX_CONNECTIONS, (soft - SPARE_DESCRIPTORS) // 2)


def admit(address, budget):
    with open_connections_lock:
        if (sum(open_connections.values()) >= budget
                or open_connections[address] >= MAX_PER_ADDRESS):
            return False
        open_connections[address] += 1
        return True


def release(address):
    with open_connections_lock:
        open_connections[address] -= 1
        if open_connections[address] == 0:
            del open_connections[address]


def announce_forever():
    """Keeps the server told where to point telnet, through server restarts and address changes."""
    while True:
        announced = listening_on
        if announced is not None:
            try:
                with socket.create_connection(BACKEND, timeout=RETRY_SECONDS) as control:
                    control.sendall(f"RELAY {announced} {PUBLIC_PORT}\r\n".encode("ascii"))
                    while listening_on == announced:
                        try:
                            control.recv(1)
                        except socket.timeout:
                            continue
                        # The server never writes here, so recv returning at all means it hung up.
                        break
            except OSError:
                pass
        time.sleep(RETRY_SECONDS)


def relay(client, source):
    """Copies what the client types to the server, until either side hangs up or goes quiet."""
    try:
        with client, socket.create_connection(BACKEND, timeout=IDLE_SECONDS) as backend:
            dest_ip, dest_port = client.getsockname()
            header = f"PROXY TCP4 {source[0]} {dest_ip} {source[1]} {dest_port}\r\n"
            backend.sendall(header.encode("ascii"))
            client.sendall(GREETING.encode("ascii"))

            poller = select.poll()
            poller.register(client, select.POLLIN)
            poller.register(backend, select.POLLIN)
            while events := poller.poll(IDLE_SECONDS * 1000):
                for fd, _ in events:
                    if fd == backend.fileno():
                        # A hang-up, or something other than TelnetServer talking. Either way, stop.
                        return
                    if data := client.recv(4096):
                        backend.sendall(data)
                    else:
                        # Pass the hang-up along, so a line typed without Enter still reaches the
                        # server, then wait for it to finish with the line and hang up in turn.
                        backend.shutdown(socket.SHUT_WR)
                        poller.unregister(client)
    except OSError:
        # The client or the container went away; either way there is nothing left to relay.
        pass
    finally:
        release(source[0])


def listen(address):
    """A listener on just the address the room is told to use, not every interface (VPNs too)."""
    try:
        listener = socket.create_server((address, PUBLIC_PORT), family=socket.AF_INET,
                                        backlog=BACKLOG)
    except OSError as e:
        return None, f"Cannot listen on {address} port {PUBLIC_PORT}: {e.strerror or e}. Retrying."
    # Wake up now and then, even when no one is connecting, to notice a change of address.
    listener.settimeout(ADDRESS_CHECK_SECONDS)
    return listener, f"The room should telnet to {address} {PUBLIC_PORT}."


def main():
    global listening_on
    budget = connection_budget()
    threading.Thread(target=announce_forever, daemon=True).start()

    listener, said, next_check = None, None, 0
    try:
        while True:
            if time.monotonic() >= next_check:
                next_check = time.monotonic() + ADDRESS_CHECK_SECONDS
                address = primary_address()
                if address is not None and address != listening_on:
                    if listener is not None:
                        listening_on = None
                        listener.close()
                    listener, message = listen(address)
                    if listener is not None:
                        listening_on = address
                    # A failure is retried every check, but only worth saying once.
                    if message != said:
                        print(message, flush=True)
                        said = message
            if listener is None:
                time.sleep(ADDRESS_CHECK_SECONDS)
                continue

            try:
                client, source = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                # Out of descriptors, or a client that gave up while queued. Neither ends the relay.
                time.sleep(0.1)
                continue

            if not admit(source[0], budget):
                client.close()
                continue
            try:
                threading.Thread(target=relay, args=(client, source), daemon=True).start()
            except RuntimeError:
                # Out of threads. Turn this one away and keep serving everyone else.
                release(source[0])
                client.close()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
