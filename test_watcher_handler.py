from pathlib import Path

import manatuabon_agent as agent_module
from manatuabon_agent import WatcherHandler


class FakeIngest:
    def __init__(self):
        self.calls = []


class FakeMovedEvent:
    def __init__(self, src_path: str, dest_path: str):
        self.src_path = src_path
        self.dest_path = dest_path
        self.is_directory = False


class ImmediateTimer:
    def __init__(self, interval, func, args=None, kwargs=None):
        self.interval = interval
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}

    def start(self):
        return None


def test_on_moved_queues_destination_path():
    original_timer = agent_module.threading.Timer
    agent_module.threading.Timer = ImmediateTimer
    try:
        handler = WatcherHandler(FakeIngest())
        event = FakeMovedEvent(
            src_path=r"D:\Manatuabon\inbox\bundle.json.tmp",
            dest_path=r"D:\Manatuabon\inbox\bundle.json",
        )
        handler.on_moved(event)
    finally:
        agent_module.threading.Timer = original_timer

    assert str(Path(event.dest_path)) in handler._pending, handler._pending


def main():
    test_on_moved_queues_destination_path()
    print("watcher handler tests passed")


if __name__ == "__main__":
    main()