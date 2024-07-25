import parsl
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.app.app import python_app


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="local_htex",
                cores_per_worker=1,
                max_workers_per_node=4,
                address=address_by_hostname(),
            )
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=55055,
            monitoring_debug=True,
            resource_monitoring_interval=1,
        ),
        strategy="none",
        usage_tracking=2,
    )


config = fresh_config()
parsl.load(config)


@python_app
def add(x: int, y: int):
    return x + y


@python_app
def sub(x: int, y: int):
    return x - y


if __name__ == "__main__":
    res = []
    for i in range(10):
        res.append(add(5, 3))
        res.append(sub(5, 3))

    for fut in res:
        print(fut.result(), end=",")
    print()
