"""Minimal systemd readiness/watchdog notification without a new dependency."""

from __future__ import annotations

import os
import socket


def notify_systemd(message: str) -> bool:
    """Send a datagram to systemd when this process belongs to a notify unit."""

    address = os.getenv("NOTIFY_SOCKET", "").strip()
    if not address:
        return False
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as client:
            client.connect(address)
            client.sendall(str(message).encode("utf-8"))
    except OSError:
        return False
    return True


__all__ = ["notify_systemd"]
