from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
import os
from typing import Iterable, Tuple, Optional, List, Sequence

import grpc
import logging

from bencherscaffold.protoclasses import second_level_services_pb2_grpc
from bencherscaffold.protoclasses.grcp_service import GRCPService as _BaseGRCPService

_DEFAULT_HOSTS: Tuple[str, str] = ("0.0.0.0", "[::]")
_DEFAULT_LISTEN_EXAMPLES: Tuple[str, str, str] = ("127.0.0.1", "[::]", "unix:/tmp/benchmark.sock")


def _normalize_hosts(listen_hosts: Optional[Iterable[str]]) -> Tuple[str, ...]:
    """
    Normalizes a list of listener hosts, handling None and removing duplicates/empty strings.
    This function has been updated to use Optional[Iterable[str]] instead of Iterable[str] | None
    and List/Tuple for consistency with Python 3.8 typing.
    """
    if listen_hosts is None:
        listen_hosts = _DEFAULT_HOSTS
    # Explicitly use List from typing for Python 3.8 compatibility
    normalized: List[str] = []
    for host in listen_hosts:
        candidate = host.strip()
        if candidate and candidate not in normalized:
            normalized.append(candidate)
    return tuple(normalized)


def grpc_target(host_or_address: str, port: int) -> str:
    if host_or_address.startswith("unix:"):
        return host_or_address
    return f"{host_or_address}:{port}"


def parse_listen_entries(raw_value: Optional[str]) -> Optional[Tuple[str, ...]]:
    if raw_value is None:
        return None
    return _normalize_hosts(raw_value.split(","))


def resolve_listen_entries(
        cli_entries: Optional[Iterable[str]],
        env_var: Optional[str] = None,
        default: Optional[Iterable[str]] = None,
) -> Tuple[str, ...]:
    if cli_entries is not None:
        return _normalize_hosts(cli_entries)

    if env_var is not None:
        env_entries = parse_listen_entries(os.environ.get(env_var))
        if env_entries is not None:
            return env_entries

    return _normalize_hosts(default)


def add_listen_argument(
        parser: ArgumentParser,
        env_var: str,
        option: str = "--listen-host",
        dest: str = "listen_hosts",
        value_name: str = "Host or gRPC address",
        examples: Sequence[str] = _DEFAULT_LISTEN_EXAMPLES,
) -> None:
    example_str = ", ".join(examples)
    parser.add_argument(
        option,
        dest=dest,
        action="append",
        default=None,
        help=f"{value_name} to listen on (for example {example_str}). "
             f"Repeat to bind multiple values. Can also be set via the {env_var} "
             "environment variable as a comma-separated list.",
    )


class DualStackGRCPService(_BaseGRCPService):

    def __init__(
            self,
            *,
            listen_hosts: Optional[Iterable[str]] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._listen_hosts = _normalize_hosts(listen_hosts)

    @property
    def listen_hosts(self) -> Tuple[str, ...]:
        return self._listen_hosts

    def serve(self):
        logging.basicConfig()
        server = grpc.server(ThreadPoolExecutor(max_workers=self.n_cores))
        second_level_services_pb2_grpc.add_SecondLevelBencherServicer_to_server(self, server)
        bound = 0
        for host in self._listen_hosts:
            bound += server.add_insecure_port(grpc_target(host, self.port))
        if bound == 0:
            raise RuntimeError(f"Could not bind benchmark service on port {self.port} for hosts {self._listen_hosts}")
        addresses = ", ".join(self._listen_hosts)
        print(f"Server started, listening on {self.port} via {addresses}")
        server.start()
        server.wait_for_termination()
