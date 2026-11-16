"""
COMP2100 Network Programming
Multithreaded TCP Server

Why multithreading?
- The main thread keeps listening for new clients.
- Each client gets its own worker thread.
- One slow client does not block all other clients.
"""

import json
import socket
import threading
from http import HTTPStatus
from pathlib import Path

HOST = "0.0.0.0"       # Listen on all available network interfaces
PORT = 9999     # All clients must use the same port


MAX_HEADER_BYTES = 16 * 1024
MAX_BODY_BYTES = 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 5
EDITOR_PATH = Path(__file__).with_name("editor.html")
document_lock = threading.Lock()
document = {"code": "", "revision": 0}


def read_request(connection):
    """Read one HTTP request and return (method, target, headers, body)."""
    with connection.makefile("rb") as stream:
        line = stream.readline(MAX_HEADER_BYTES + 1)
        if not line:
            return None
        remaining = MAX_HEADER_BYTES - len(line)
        if remaining < 0 or not line.endswith(b"\r\n"):
            raise ValueError("Invalid request line")
        method, target, version = line[:-2].decode("ascii").split(" ")
        if not method or not target or version not in ("HTTP/1.0", "HTTP/1.1"):
            raise ValueError("Invalid request line")

        headers = {}
        while True:
            line = stream.readline(remaining + 1)
            remaining -= len(line)
            if remaining < 0 or not line.endswith(b"\r\n"):
                raise ValueError("Invalid or oversized headers")
            if line == b"\r\n":
                break
            name, separator, value = line[:-2].decode("iso-8859-1").partition(":")
            if not separator or not name or any(char.isspace() for char in name):
                raise ValueError("Invalid header")
            name = name.lower()
            if name in ("content-length", "host") and name in headers:
                raise ValueError("Duplicate header")
            headers[name] = value.strip()

        if version == "HTTP/1.1" and not headers.get("host"):
            raise ValueError("Missing Host header")
        if "transfer-encoding" in headers:
            raise ValueError("Transfer-Encoding is unsupported")

        length = headers.get("content-length", "0")
        if not length.isascii() or not length.isdecimal():
            raise ValueError("Invalid Content-Length")
        # Compare lengths before converting an arbitrarily large decimal string.
        length = length.lstrip("0") or "0"
        if len(length) > len(str(MAX_BODY_BYTES)) or int(length) > MAX_BODY_BYTES:
            raise OverflowError("Request body is too large")
        body = stream.read(int(length))
        if len(body) != int(length):
            raise ValueError("Incomplete request body")

    return method, target, headers, body


def respond(connection, status, body, content_type="text/plain; charset=utf-8"):
    """Send an HTTP response with a byte-accurate Content-Length."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    headers = (
        f"HTTP/1.1 {status.value} {status.phrase}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    connection.sendall(headers.encode("ascii") + body)


def handle_client(connection, address):
    """Handle one HTTP request in this client's worker thread."""
    with connection:
        connection.settimeout(REQUEST_TIMEOUT_SECONDS)
        try:
            try:
                request = read_request(connection)
            except ValueError:
                respond(connection, HTTPStatus.BAD_REQUEST, "Invalid HTTP request")
                return
            except OverflowError:
                respond(connection, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request body is too large")
                return
            except TimeoutError:
                respond(connection, HTTPStatus.REQUEST_TIMEOUT, "Request timed out")
                return

            if request is None:
                return

            method, target, _, body = request
            if method == "GET" and target == "/":
                respond(connection, HTTPStatus.OK, EDITOR_PATH.read_bytes(), "text/html; charset=utf-8")
            elif method == "GET" and target == "/code":
                with document_lock:
                    snapshot = json.dumps(document)
                respond(connection, HTTPStatus.OK, snapshot, "application/json")
            elif method == "POST" and target == "/code":
                save_document(connection, body)
            else:
                respond(connection, HTTPStatus.NOT_FOUND, "Not found")
        except OSError as error:
            print(f"Connection with {address}: {error}")


def save_document(connection, body):
    try:
        update = json.loads(body)
        if not isinstance(update, dict) or not isinstance(update.get("code"), str):
            raise ValueError("Expected a code string")
    except ValueError:
        respond(connection, HTTPStatus.BAD_REQUEST, "Expected JSON with a code string")
        return

    with document_lock:
        document["code"] = update["code"]
        document["revision"] += 1
    respond(connection, HTTPStatus.OK, "Saved")


def start_server():
    """Create the TCP server and accept many clients."""

    # AF_INET = IPv4
    # SOCK_STREAM = TCP
    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    # Lets us restart the server without waiting for the port.
    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    # Attach the socket to the local machine and port 9999.
    server_socket.bind((HOST, PORT))

    # Start listening for connection requests.
    server_socket.listen()

    print("==========================================")
    print("MULTITHREADED TCP SERVER")
    print("==========================================")
    print(f"Listening on port {PORT}")
    print("Waiting for clients...")
    print("Each client gets its own thread.")
    print("==========================================")

    try:
        while True:
            # MAIN THREAD waits for a NEW client.
            connection, address = server_socket.accept()

            print(
                f"\nMAIN THREAD: new client "
                f"{address[0]}:{address[1]}"
            )

            # Create a new thread for this client.
            client_thread = threading.Thread(
                target=handle_client,
                args=(connection, address),
                daemon=True
            )

            # Give the thread a readable name.
            client_thread.name = (
                f"ClientThread-{address[1]}"
            )

            # Start handle_client() in parallel.
            client_thread.start()

            print(
                "MAIN THREAD: worker started. "
                f"Active threads = {threading.active_count()}"
            )

            # IMPORTANT:
            # The main thread immediately loops back to accept()
            # so another client can connect.

    except KeyboardInterrupt:
        print("\nServer stopped.")

    finally:
        server_socket.close()


if __name__ == "__main__":
    start_server()
