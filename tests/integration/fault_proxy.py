"""A TCP proxy a test puts between a client and a real server to inject network faults (phase 7,
decision I1).

The client is pointed at `proxy.port` instead of the server. Modes:

- forward: bytes pass both ways (the default).
- refuse(): the listening socket is closed, so new connections are refused as by a stopped
  server; open connections are left as they are. forward() listens on the same port again.
- cut(): every open connection is closed on both sides, as when a server process dies.
- hang(): connections stay open, new ones are accepted, but nothing is passed on in either
  direction, as with a server that stopped answering. The kernel still acknowledges the bytes,
  so TCP keepalives cannot detect it. What arrived meanwhile is passed on after forward(), in
  order, as by a paused server that resumes.
- cut_on(marker, forward=...): one-shot. When a client chunk contains `marker`, either close the
  connection without passing the chunk on (the server never sees it), or pass it on, wait for
  the server's answer, and close the connection without relaying the answer (the server acted,
  the client never learns it). Chunks held during hang() are checked too, when passed on.

Standard library only: threads and blocking sockets. A marker split across two reads is not
seen; the markers used are a few bytes, sent as one protocol message.

A fault test that passes because the proxy broke would prove nothing, so the proxy never fails
quietly: an unexpected exception in one of its threads, an accept that fails outside refuse() or
close(), or an upstream connection that cannot be opened is recorded, and close() (the end of the
`with` block, so the test's fixture) raises them.
"""

import contextlib
import select
import socket
import threading
from dataclasses import dataclass, field
from typing import Literal, Self

_CHUNK = 65536
# How long a thread waits for bytes before it looks at the mode again.
_IDLE_MS = 50
_UPSTREAM_CONNECT_TIMEOUT_S = 5.0

Mode = Literal["forward", "refuse", "hang"]


class FaultProxyError(Exception):
    """The proxy itself failed; the fault the test meant to inject may not have happened."""


@dataclass
class _CutOn:
    marker: bytes
    forward: bool
    fired: threading.Event = field(default_factory=threading.Event)


class _Link:
    """One client connection and its upstream connection."""

    def __init__(self, client: socket.socket, upstream: socket.socket) -> None:
        self.client = client
        self.upstream = upstream
        self.closed = threading.Event()
        # Set by the client→server thread before it sends the marked chunk, so the server's
        # answer, which can only come after, always finds it set.
        self.drop_answer: _CutOn | None = None

    def close(self) -> None:
        self.closed.set()
        for sock in (self.client, self.upstream):
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()


class FaultProxy:
    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self.upstream = (upstream_host, upstream_port)
        self.host = "127.0.0.1"
        self.errors: list[BaseException] = []
        self._lock = threading.Lock()
        self._mode: Mode = "forward"
        self._links: list[_Link] = []
        self._cut_on: _CutOn | None = None
        # Counts cut() calls, so a connection accepted before a cut but registered after it is
        # closed too instead of surviving the cut.
        self._cuts = 0
        self._stopped = False
        self._listener = self._listen(0)
        self.port: int = self._listener.getsockname()[1]
        self._start_accepting()

    # --- Modes ------------------------------------------------------------------------------------

    def forward(self) -> None:
        with self._lock:
            was_refusing = self._mode == "refuse"
            if was_refusing:
                # The new listener before the mode, under one lock: the accept thread woken by
                # refuse() must never see "forward" with the old listener, or it takes its
                # wake-up for a failure.
                self._listener = self._listen(self.port)
            self._mode = "forward"
        if was_refusing:
            self._start_accepting()

    def refuse(self) -> None:
        with self._lock:
            if self._mode == "refuse":
                return
            self._mode = "refuse"
        self._stop_listening()

    def hang(self) -> None:
        with self._lock:
            self._mode = "hang"

    def cut(self) -> None:
        with self._lock:
            self._cuts += 1
            links, self._links = self._links, []
        for link in links:
            link.close()

    def cut_on(self, marker: bytes, *, forward: bool) -> threading.Event:
        """Arm the one-shot cut; the returned event is set once it fired."""
        armed = _CutOn(marker, forward)
        with self._lock:
            self._cut_on = armed
        return armed.fired

    def close(self) -> None:
        """Stop and close every connection; raise what went wrong inside the proxy, if anything."""
        with self._lock:
            self._stopped = True
            mode = self._mode
        if mode != "refuse":
            self._stop_listening()
        self.cut()
        with self._lock:
            errors = list(self.errors)
        if errors:
            raise FaultProxyError(f"the fault proxy failed: {errors!r}") from errors[0]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def open_connections(self) -> int:
        with self._lock:
            return sum(1 for link in self._links if not link.closed.is_set())

    # --- Plumbing ---------------------------------------------------------------------------------

    def _record(self, exc: BaseException) -> None:
        with self._lock:
            self.errors.append(exc)

    def _listen(self, port: int) -> socket.socket:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Listening again on the same port after refuse(), with the old connections' TIME_WAIT.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, port))
        listener.listen()
        return listener

    def _stop_listening(self) -> None:
        # close() alone does not stop a socket another thread is blocked in accept() on: the
        # blocked call keeps it open and listening. shutdown() wakes that call and stops it.
        with contextlib.suppress(OSError):
            self._listener.shutdown(socket.SHUT_RDWR)
        self._listener.close()

    def _start_accepting(self) -> None:
        threading.Thread(target=self._accept, args=(self._listener,), daemon=True).start()

    def _connect_upstream(self) -> socket.socket:
        upstream = socket.create_connection(self.upstream, timeout=_UPSTREAM_CONNECT_TIMEOUT_S)
        upstream.settimeout(None)
        return upstream

    def _accept(self, listener: socket.socket) -> None:
        try:
            self._accept_until_closed(listener)
        except BaseException as exc:
            self._record(exc)

    def _accept_until_closed(self, listener: socket.socket) -> None:
        while True:
            try:
                client, _ = listener.accept()
            except OSError as exc:
                with self._lock:
                    expected = (
                        self._stopped or self._mode == "refuse" or listener is not self._listener
                    )
                if not expected:
                    self._record(exc)
                return
            with self._lock:
                cuts_before = self._cuts
            try:
                upstream = self._connect_upstream()
            except OSError as exc:
                self._record(exc)
                client.close()
                continue
            link = _Link(client, upstream)
            with self._lock:
                stopped = self._stopped
                gone = stopped or self._mode == "refuse" or self._cuts != cuts_before
                if not gone:
                    self._links.append(link)
            if gone:
                link.close()
                if stopped:
                    return
                continue
            for source, target, upwards in ((client, upstream, True), (upstream, client, False)):
                threading.Thread(
                    target=self._pump, args=(link, source, target, upwards), daemon=True
                ).start()

    def _pump(
        self, link: _Link, source: socket.socket, target: socket.socket, upwards: bool
    ) -> None:
        """Pass `source`'s bytes to `target` until the link closes."""
        try:
            self._pump_until_closed(link, source, target, upwards)
        except BaseException as exc:
            self._record(exc)
        finally:
            link.close()

    def _pump_until_closed(
        self, link: _Link, source: socket.socket, target: socket.socket, upwards: bool
    ) -> None:
        fd = source.fileno()
        if fd == -1:  # closed before this thread started
            return
        # poll, not select: select refuses descriptors from 1024 up, which a long test session
        # reaches.
        poller = select.poll()
        poller.register(fd, select.POLLIN)
        held: list[bytes] = []
        while not link.closed.is_set():
            if held and not self._hanging():
                for chunk in held:
                    if not self._pass_on(link, chunk, target, upwards):
                        return
                held.clear()
            data = self._receive(link, source, poller)
            if data is None:
                return
            if not data:
                continue
            # While anything is held, new bytes queue behind it: forward() may have been called
            # after this loop's flush, and sending them now would overtake the held ones.
            if held or self._hanging():
                held.append(data)
                continue
            if not self._pass_on(link, data, target, upwards):
                return

    def _receive(self, link: _Link, source: socket.socket, poller: select.poll) -> bytes | None:
        """The next chunk, b"" when nothing arrived yet, None when the link is finished."""
        if not poller.poll(_IDLE_MS):
            return b""
        try:
            data = source.recv(_CHUNK)
        except OSError:  # reset by the peer, or closed by cut() meanwhile
            link.close()
            return None
        if not data:
            link.close()
            return None
        return data

    def _hanging(self) -> bool:
        with self._lock:
            return self._mode == "hang"

    def _pass_on(self, link: _Link, data: bytes, target: socket.socket, upwards: bool) -> bool:
        """Send `data` on, applying cut_on; False once the link is finished."""
        if upwards:
            with self._lock:
                armed = self._cut_on
                if armed is not None and armed.marker in data:
                    self._cut_on = None
                else:
                    armed = None
            if armed is not None and not armed.forward:
                link.close()
                armed.fired.set()
                return False
            if armed is not None:
                link.drop_answer = armed  # the answer cuts the link, in the other thread
        elif link.drop_answer is not None:
            link.close()
            link.drop_answer.fired.set()
            return False
        try:
            target.sendall(data)
        except OSError:
            link.close()
            return False
        return True
