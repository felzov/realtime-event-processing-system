"""The fault proxy itself (phase 7 criterion 1), against a local echo server: each mode does what
the database and broker tests rely on."""

import contextlib
import os
import socket
import threading
import time
from collections.abc import Iterator

import pytest

from tests.integration.fault_proxy import FaultProxy, FaultProxyError

TIMEOUT_S = 5.0
# Long enough that bytes the proxy would pass on have arrived; short enough for a quick test.
QUIET_S = 0.5


class EchoServer:
    """Sends back every chunk it receives, and records what it received."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port: int = self.listener.getsockname()[1]
        self.received = bytearray()
        self.lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._echo, args=(conn,), daemon=True).start()

    def _echo(self, conn: socket.socket) -> None:
        # A connection the proxy cuts ends here with a reset; that is the test working.
        with conn, contextlib.suppress(OSError):
            while data := conn.recv(65536):
                with self.lock:
                    self.received += data
                conn.sendall(data)

    def close(self) -> None:
        # shutdown() first: close() alone leaves a socket listening while _accept is blocked on it.
        with contextlib.suppress(OSError):
            self.listener.shutdown(socket.SHUT_RDWR)
        self.listener.close()

    def got(self) -> bytes:
        with self.lock:
            return bytes(self.received)


@pytest.fixture
def echo() -> Iterator[EchoServer]:
    server = EchoServer()
    yield server
    server.close()


@pytest.fixture
def proxy(echo: EchoServer) -> Iterator[FaultProxy]:
    with FaultProxy("127.0.0.1", echo.port) as running:
        yield running


def connect(proxy: FaultProxy) -> socket.socket:
    return socket.create_connection((proxy.host, proxy.port), timeout=TIMEOUT_S)


def nothing_arrives(sock: socket.socket) -> bool:
    sock.settimeout(QUIET_S)
    try:
        sock.recv(1)
    except TimeoutError:
        return True
    return False


def closed_by_peer(sock: socket.socket) -> bool:
    sock.settimeout(TIMEOUT_S)
    try:
        return sock.recv(1) == b""
    except ConnectionResetError:
        return True


def test_forward_passes_bytes_both_ways(proxy: FaultProxy, echo: EchoServer) -> None:
    with connect(proxy) as sock:
        sock.sendall(b"ping")
        assert sock.recv(4) == b"ping"
    assert echo.got() == b"ping"


def test_refuse_rejects_new_connections_until_forward_listens_again(
    proxy: FaultProxy,
) -> None:
    with connect(proxy) as before:
        # Let the proxy's accept thread block in accept() again: a listener closed under it kept
        # listening, and only a refuse() right after a connection happened to work.
        time.sleep(QUIET_S)
        proxy.refuse()
        with pytest.raises(ConnectionRefusedError):
            connect(proxy)
        # Open connections are not touched.
        before.sendall(b"still")
        assert before.recv(5) == b"still"

    proxy.forward()
    with connect(proxy) as after:
        after.sendall(b"back")
        assert after.recv(4) == b"back"


def test_cut_closes_open_connections_and_accepts_new_ones(proxy: FaultProxy) -> None:
    with connect(proxy) as sock:
        sock.sendall(b"x")
        assert sock.recv(1) == b"x"

        proxy.cut()

        assert closed_by_peer(sock)
    with connect(proxy) as again:
        again.sendall(b"y")
        assert again.recv(1) == b"y"


def test_hang_passes_nothing_on_until_forward(proxy: FaultProxy, echo: EchoServer) -> None:
    with connect(proxy) as sock:
        proxy.hang()
        sock.sendall(b"held")

        assert nothing_arrives(sock)
        assert echo.got() == b""
        assert proxy.open_connections == 1

        proxy.forward()
        sock.settimeout(TIMEOUT_S)
        assert sock.recv(4) == b"held"


def test_cut_on_without_forwarding_never_sends_the_marked_chunk(
    proxy: FaultProxy, echo: EchoServer
) -> None:
    fired = proxy.cut_on(b"MARK", forward=False)
    with connect(proxy) as sock:
        sock.sendall(b"before")
        assert sock.recv(6) == b"before"
        sock.sendall(b"with MARK")

        assert fired.wait(TIMEOUT_S)
        assert closed_by_peer(sock)
    assert echo.got() == b"before"


def test_cut_on_with_forwarding_sends_the_chunk_and_swallows_the_answer(
    proxy: FaultProxy, echo: EchoServer
) -> None:
    fired = proxy.cut_on(b"MARK", forward=True)
    with connect(proxy) as sock:
        sock.sendall(b"with MARK")

        assert fired.wait(TIMEOUT_S)
        assert closed_by_peer(sock)  # the echo never reached the client
    assert echo.got() == b"with MARK"


def test_cut_on_fires_once(proxy: FaultProxy, echo: EchoServer) -> None:
    fired = proxy.cut_on(b"MARK", forward=False)
    with connect(proxy) as first:
        first.sendall(b"MARK")
        assert fired.wait(TIMEOUT_S)
    with connect(proxy) as second:
        second.sendall(b"MARK again")
        assert second.recv(10) == b"MARK again"


def test_bytes_held_during_hang_reach_the_server_before_later_ones(
    proxy: FaultProxy, echo: EchoServer
) -> None:
    # forward() can come while a proxy thread waits for bytes; what arrives right after it must
    # not overtake what was held (it did, in most rounds, before the fix).
    rounds = 20
    with connect(proxy) as sock:
        for _ in range(rounds):
            proxy.hang()
            sock.sendall(b"A")
            time.sleep(0.1)
            proxy.forward()
            sock.sendall(b"B")
        sock.settimeout(TIMEOUT_S)
        echoed = b""
        while len(echoed) < 2 * rounds:
            echoed += sock.recv(2 * rounds)

    assert echo.got() == b"AB" * rounds
    assert echoed == b"AB" * rounds


def test_a_marker_held_during_hang_still_fires(proxy: FaultProxy, echo: EchoServer) -> None:
    fired = proxy.cut_on(b"MARK", forward=False)
    with connect(proxy) as sock:
        proxy.hang()
        sock.sendall(b"MARK")
        time.sleep(QUIET_S)

        proxy.forward()

        assert fired.wait(TIMEOUT_S)
        assert closed_by_peer(sock)
    assert echo.got() == b""


def test_forwarding_works_with_descriptors_above_1024(proxy: FaultProxy) -> None:
    # select() refuses descriptors from 1024 up; a long session reaches them. Occupy the low
    # numbers so the proxy's sockets for this connection get high ones.
    occupied = [os.dup(0) for _ in range(1100)]
    try:
        with connect(proxy) as sock:
            assert sock.fileno() > 1024
            sock.sendall(b"high")
            assert sock.recv(4) == b"high"
    finally:
        for fd in occupied:
            os.close(fd)
    assert proxy.errors == []


def test_an_unreachable_upstream_is_reported_when_the_proxy_closes() -> None:
    # Not a silent EOF: the test would otherwise fail later as if the server were down.
    with socket.create_server(("127.0.0.1", 0)) as placeholder:
        closed_port: int = placeholder.getsockname()[1]
    proxy = FaultProxy("127.0.0.1", closed_port)
    with connect(proxy) as sock:
        assert closed_by_peer(sock)

    with pytest.raises(FaultProxyError) as exc_info:
        proxy.close()
    assert isinstance(exc_info.value.__cause__, ConnectionRefusedError)


class SlowUpstreamProxy(FaultProxy):
    """Holds each upstream connect until `release` is set, so a cut can come in between."""

    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self.connecting = threading.Event()
        self.release = threading.Event()
        super().__init__(upstream_host, upstream_port)

    def _connect_upstream(self) -> socket.socket:
        self.connecting.set()
        assert self.release.wait(TIMEOUT_S)
        return super()._connect_upstream()


def test_a_connection_accepted_before_a_cut_does_not_survive_it(echo: EchoServer) -> None:
    with SlowUpstreamProxy("127.0.0.1", echo.port) as proxy, connect(proxy) as sock:
        assert proxy.connecting.wait(TIMEOUT_S)
        proxy.cut()
        proxy.release.set()

        assert closed_by_peer(sock)
        assert proxy.open_connections == 0


def test_refuse_then_forward_at_once_is_not_reported_as_a_proxy_failure(
    echo: EchoServer,
) -> None:
    # The accept thread woken by refuse() used to check the mode after forward() changed it but
    # before forward() replaced the listener, and recorded its wake-up as a failure (58 of 300).
    for _ in range(100):
        proxy = FaultProxy("127.0.0.1", echo.port)
        time.sleep(0.005)  # the accept thread is blocked in accept()
        proxy.refuse()
        proxy.forward()
        time.sleep(0.01)
        proxy.close()  # raises FaultProxyError if anything was recorded
