from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Tuple

import grpc
import logging

from bencherscaffold.protoclasses import second_level_services_pb2_grpc
from bencherscaffold.protoclasses.grcp_service import GRCPService as _BaseGRCPService

_DEFAULT_HOSTS: Tuple[str, str] = ("0.0.0.0", "[::]")


def _normalize_hosts(listen_hosts: Iterable[str] | None) -> tuple[str, ...]:
    if listen_hosts is None:
        listen_hosts = _DEFAULT_HOSTS
    normalized: list[str] = []
    for host in listen_hosts:
        candidate = host.strip()
        if candidate and candidate not in normalized:
            normalized.append(candidate)
    return tuple(normalized)


class DualStackGRCPService(_BaseGRCPService):

    def __init__(
            self,
            *,
            listen_hosts: Iterable[str] | None = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._listen_hosts = _normalize_hosts(listen_hosts)

    @property
    def listen_hosts(self) -> tuple[str, ...]:
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
