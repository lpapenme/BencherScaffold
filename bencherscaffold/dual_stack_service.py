from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Tuple, Optional, List

import grpc
import logging

from bencherscaffold.protoclasses import second_level_services_pb2_grpc
from bencherscaffold.protoclasses.grcp_service import GRCPService as _BaseGRCPService

_DEFAULT_HOSTS: Tuple[str, str] = ("0.0.0.0", "[::]")


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
            bound += server.add_insecure_port(f"{host}:{self.port}")
        if bound == 0:
            raise RuntimeError(f"Could not bind benchmark service on port {self.port} for hosts {self._listen_hosts}")
        addresses = ", ".join(self._listen_hosts)
        print(f"Server started, listening on {self.port} via {addresses}")
        server.start()
        server.wait_for_termination()
