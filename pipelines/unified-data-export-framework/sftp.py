"""Secret resolution and secure SFTP upload."""

from __future__ import annotations

import io
import os
import socket
from pathlib import Path

import paramiko
from google.api_core import exceptions as google_exceptions
from google.api_core.retry import Retry, if_exception_type
from google.cloud import secretmanager
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from exceptions import ExportTimeoutError, SFTPError, SecretResolutionError
from models import ExportRequest
from utils import remaining_seconds

SECRET_RETRY = Retry(
    predicate=if_exception_type(
        google_exceptions.TooManyRequests,
        google_exceptions.InternalServerError,
        google_exceptions.ServiceUnavailable,
    ),
    deadline=60.0,
)


class SecretResolver:
    """Resolve password or private-key material from Secret Manager."""

    def __init__(self, client: secretmanager.SecretManagerServiceClient | None = None) -> None:
        self._client = client or secretmanager.SecretManagerServiceClient()

    def resolve(self, secret: str, project_id: str) -> str:
        """Read `latest` from a short secret name or a full version resource."""
        name = secret if secret.startswith("projects/") else f"projects/{project_id}/secrets/{secret}/versions/latest"
        if "/versions/" not in name:
            name = f"{name}/versions/latest"
        try:
            response = self._client.access_secret_version(
                request={"name": name}, retry=SECRET_RETRY, timeout=30
            )
            return response.payload.data.decode("utf-8")
        except Exception as exc:
            raise SecretResolutionError(f"unable to resolve Secret Manager value: {exc}") from exc


class SFTPUploader:
    """Upload a local file through a verified Paramiko SSH transport."""

    def __init__(self, resolver: SecretResolver | None = None) -> None:
        self._resolver = resolver or SecretResolver()

    @retry(
        retry=retry_if_exception_type((OSError, EOFError, paramiko.SSHException)),
        wait=wait_exponential_jitter(initial=1, max=15),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _put_once(
        self, request: ExportRequest, local_path: Path, remote_path: str, deadline: float
    ) -> None:
        timeout = min(remaining_seconds(deadline, 5), 60.0)
        socket_connection = socket.create_connection(
            (request.sftp_host, request.sftp_port), timeout=timeout
        )
        transport = paramiko.Transport(socket_connection)
        try:
            transport.banner_timeout = timeout
            transport.auth_timeout = timeout
            transport.start_client(timeout=timeout)
            self._verify_host_key(transport, request)
            if request.sftp_private_key_secret:
                material = self._resolver.resolve(request.sftp_private_key_secret, request.project_id)
                key = self._load_private_key(material)
                transport.auth_publickey(request.sftp_username, key)
            else:
                password = request.sftp_password or self._resolver.resolve(
                    request.sftp_secret or "", request.project_id
                )
                transport.auth_password(request.sftp_username, password)
            with paramiko.SFTPClient.from_transport(transport) as client:
                client.get_channel().settimeout(remaining_seconds(deadline, 5))
                if not request.overwrite:
                    try:
                        existing = client.stat(remote_path)
                        if existing.st_size == local_path.stat().st_size:
                            return
                        raise SFTPError("remote file exists with a different size")
                    except FileNotFoundError:
                        pass
                partial = f"{remote_path}.{request.idempotency_key[:12]}.part"
                client.put(str(local_path), partial, confirm=True)
                if request.overwrite:
                    try:
                        client.remove(remote_path)
                    except FileNotFoundError:
                        pass
                client.rename(partial, remote_path)
        finally:
            transport.close()

    def upload(
        self, request: ExportRequest, local_path: Path, remote_path: str, deadline: float
    ) -> None:
        """Upload with transient retries while respecting the request deadline."""
        try:
            self._put_once(request, local_path, remote_path, deadline)
        except ExportTimeoutError:
            raise
        except Exception as exc:
            raise SFTPError(f"SFTP upload failed: {exc}") from exc

    @staticmethod
    def _verify_host_key(transport: paramiko.Transport, request: ExportRequest) -> None:
        expected = request.sftp_host_key or os.getenv("SFTP_HOST_KEY")
        if not expected:
            raise SFTPError("sftp_host_key is required for host identity verification")
        entry = paramiko.hostkeys.HostKeyEntry.from_line(expected)
        if entry is None or not entry.key:
            raise SFTPError("sftp_host_key must be an OpenSSH known_hosts line")
        actual = transport.get_remote_server_key()
        if actual.get_name() != entry.key.get_name() or actual.asbytes() != entry.key.asbytes():
            raise SFTPError("SFTP host key verification failed")

    @staticmethod
    def _load_private_key(material: str) -> paramiko.PKey:
        for key_type in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
            try:
                return key_type.from_private_key(io.StringIO(material))
            except (paramiko.SSHException, ValueError):
                continue
        raise SecretResolutionError("private key secret contains no supported key")
