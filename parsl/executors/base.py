import logging
import os
from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from typing import Any, Callable, Dict, Optional

from typing_extensions import Literal, Self

from parsl.monitoring.radios import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
    UDPRadio,
)

logger = logging.getLogger(__name__)


class ParslExecutor(metaclass=ABCMeta):
    """Executors are abstractions that represent available compute resources
    to which you could submit arbitrary App tasks.

    This is an abstract base class that only enforces concrete implementations
    of functionality by the child classes.

    Can be used as a context manager. On exit, calls ``self.shutdown()`` with
    no arguments and re-raises any thrown exception.

    In addition to the listed methods, a ParslExecutor instance must always
    have these member fields:

       label: str - a human readable label for the executor, unique
              with respect to other executors.

       remote_monitoring_radio_config: RadioConfig describing how tasks on this executor
              should report task resource status

    An executor may optionally expose:

       storage_access: List[parsl.data_provider.staging.Staging] - a list of staging
              providers that will be used for file staging. In the absence of this
              attribute, or if this attribute is `None`, then a default value of
              ``parsl.data_provider.staging.default_staging`` will be used by the
              staging code.

              Typechecker note: Ideally storage_access would be declared on executor
              __init__ methods as List[Staging] - however, lists are by default
              invariant, not co-variant, and it looks like @typeguard cannot be
              persuaded otherwise. So if you're implementing an executor and want to
              @typeguard the constructor, you'll have to use List[Any] here.
    """

    label: str = "undefined"

    def __init__(
        self,
        *,

        # TODO: I'd like these two to go away but they're needed right now
        # to configure the interchange monitoring radio, that is
        # in addition to the submit and worker monitoring radios (!). They
        # are effectivley a third monitoring radio config, though, so what
        # should that look like for the interchange?
        hub_address: Optional[str] = None,
        hub_zmq_port: Optional[int] = None,
        submit_monitoring_radio: Optional[MonitoringRadioSender] = None,
        run_dir: str = ".",
        run_id: Optional[str] = None,
    ):
        self.hub_address = hub_address
        self.hub_zmq_port = hub_zmq_port

        # these are parameters for the monitoring radio to be used on the remote side
        # eg. in workers - to send results back, and they should end up encapsulated
        # inside a RadioConfig.
        self.submit_monitoring_radio = submit_monitoring_radio
        self.remote_monitoring_radio_config: RadioConfig = UDPRadio()

        self.run_dir = os.path.abspath(run_dir)
        self.run_id = run_id

        # will be set externally later, which is pretty ugly
        self.monitoring_receiver: Optional[MonitoringRadioReceiver] = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self.shutdown()
        return False

    @abstractmethod
    def start(self) -> None:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        pass

    @abstractmethod
    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Any) -> Future:
        """Submit.

        The executor can optionally set a parsl_executor_task_id attribute on
        the Future that it returns, and in that case, parsl will log a
        relationship between the executor's task ID and parsl level try/task
        IDs.
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        logger.debug("Starting base executor shutdown")
        # logger.error(f"BENC: monitoring receiver on {self} is {self.monitoring_receiver}")
        if self.monitoring_receiver is not None:
            logger.debug("Starting monitoring receiver shutdown")
            self.monitoring_receiver.shutdown()
            logger.debug("Done with monitoring receiver shutdown")
        logger.debug("Done with base executor shutdown")

    def monitor_resources(self) -> bool:
        """Should resource monitoring happen for tasks on running on this executor?

        Parsl resource monitoring conflicts with execution styles which use threads, and
        can deadlock while running.

        This function allows resource monitoring to be disabled per executor implementation.
        """
        return True

    @property
    def run_dir(self) -> str:
        """Path to the run directory.
        """
        return self._run_dir

    @run_dir.setter
    def run_dir(self, value: str) -> None:
        self._run_dir = value

    @property
    def run_id(self) -> Optional[str]:
        """UUID for the enclosing DFK.
        """
        return self._run_id

    @run_id.setter
    def run_id(self, value: Optional[str]) -> None:
        self._run_id = value

    @property
    def hub_address(self) -> Optional[str]:
        """Address to the Hub for monitoring.
        """
        return self._hub_address

    @hub_address.setter
    def hub_address(self, value: Optional[str]) -> None:
        self._hub_address = value

    @property
    def hub_zmq_port(self) -> Optional[int]:
        """Port to the Hub for monitoring.
        """
        return self._hub_zmq_port

    @hub_zmq_port.setter
    def hub_zmq_port(self, value: Optional[int]) -> None:
        self._hub_zmq_port = value

    @property
    def submit_monitoring_radio(self) -> Optional[MonitoringRadioSender]:
        """Local radio for sending monitoring messages
        """
        return self._submit_monitoring_radio

    @submit_monitoring_radio.setter
    def submit_monitoring_radio(self, value: Optional[MonitoringRadioSender]) -> None:
        self._submit_monitoring_radio = value
