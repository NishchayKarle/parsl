import logging
import py_chronolog_client
import time


class ChronologHandler(logging.Handler):
    def __init__(
        self,
        protocol: str,
        ip: str,
        port: int,
        provider_id: int,
    ):
        logging.Handler.__init__(self)
        self.protocol = protocol
        self.ip = ip
        self.port = port
        self.provider_id = provider_id

        self.client_conf = py_chronolog_client.ClientPortalServiceConf(
            self.protocol, self.ip, self.port, self.provider_id
        )
        self.client = py_chronolog_client.Client(self.client_conf)
        assert self.client.Connect() == 0, "Client connection failed"

        attrs = {}
        self.chronicle_name = f"nk_test_logger_chronicle_{time.time()}"
        assert (
            self.client.CreateChronicle(self.chronicle_name, attrs, 1) == 0
        ), "Chronicle creation failed"

        self.story_name = f"nk_test_logger_story_{time.time()}"
        return_tuple = self.client.AcquireStory(
            self.chronicle_name, self.story_name, attrs, 1
        )
        assert return_tuple[0] == 0, "Story acquisition failed"
        self.story_handle = return_tuple[1]

    def emit(self, record: logging.LogRecord) -> None:
        identifier = "nk_py_custom_chronolog_logger"
        self.story_handle.log_event(f"{identifier} {record.getMessage()}")

    def __del__(self):
        self.client.ReleaseStory(self.chronicle_name, self.story_name)
        self.client.Disconnect()
