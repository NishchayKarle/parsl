import logging
import pickle
import time
from multiprocessing.queues import Queue
from typing import Any, Optional

import py_chronolog_client

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)

logger = logging.getLogger(__name__)


class ChronologRadio(RadioConfig):
    client = None

    def __init__(
        self,
        *,
        services: Optional[str] = None,
        port: Optional[int] = None,
        provider_id: Optional[int] = None,
        chronicle: str = None,
        chronicle_attrs: Optional[dict] = None,
        flags: Optional[int] = None,
    ):
        self.services = services
        self.port = port
        self.provider_id = provider_id
        self.chronicle = chronicle
        self.chronicle_attrs = chronicle_attrs
        self.chronicle_flags = flags

    def create_sender(self, *, source_id: int) -> MonitoringRadioSender:
        assert (
            self.client is not None
        ), "Chronolog client should be initialized by create_receiver"
        assert (
            self.chronicle is not None
        ), "Chronicle should be initialized by create_receiver"
        return ChronologRadioSender(
            self.client,
            self.chronicle,
            self.chronicle_attrs,
            self.chronicle_flags,
            source_id,
        )

    def create_receiver(self, ip: str, resource_msgs: Queue) -> Any:
        self.ip = ip

        if self.services is None:
            self.services = "ofi+sockets"

        if self.port is None:
            self.port = 5555

        if self.provider_id is None:
            self.provider_id = 55

        client_config = py_chronolog_client.ClientPortalServiceConf(
            self.services, self.ip, self.port, self.provider_id
        )
        self.client = py_chronolog_client.Client(client_config)
        assert self.client.Connect() == 0, "Chronolog client failed to connect"

        if self.chronicle is None:
            self.chronicle = f"nk_chronicle_{time.time()}"

        if self.chronicle_attrs is None:
            self.chronicle_attrs = {}

        if self.chronicle_flags is None:
            self.chronicle_flags = 1

        assert (
            self.client.CreateChronicle(
                self.chronicle,
                self.chronicle_attrs,
                self.chronicle_flags,
            )
            == 0
        ), "Chronicle creation failed"

        return ChronologRadioReceiver(self.client, self.chronicle)


class ChronologRadioSender(MonitoringRadioSender):

    def __init__(
        self,
        client,
        chronicle: str,
        chronicle_attrs: dict,
        chronicle_flags: int,
        source_id: int,
    ) -> None:
        self.client = client

        self.story = f"nk_story_{time.time()}"
        self.story_handle, err = self.client.AcquireStory(
            chronicle, self.story, chronicle_attrs, chronicle_flags
        )
        assert err == 0, f"Failed to acquire story: {err}"

    def send(self, message: object) -> None:
        self.story_handle.log_event(f"nk_message_{pickle.dumps(message)}")

    def __del__(self):
        self.client.ReleaseStory(self.chronicle, self.story)


class ChronologRadioReceiver(MonitoringRadioReceiver):
    def __init__(self, client, chronicle: str) -> None:
        self.client = client
        self.chronicle = chronicle

    def shutdown(self) -> None:
        self.client.DestroyChronicle(self.chronicle)
        self.client.Disconnect()
