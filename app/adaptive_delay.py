
import asyncio
import logging
import time

log = logging.getLogger(__name__)


class AdaptiveDelay:
    def __init__(self, target: float, initial_sleep: float = 0.0, min_sleep: float = 0.0, max_sleep: float = 10.0):
        self.target = target
        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self.sleep_time = max(min_sleep, min(max_sleep, initial_sleep))
        self.samples = []
        self.next_available_at = 0.0

    def time_until_ready(self) -> float:
        return max(0.0, self.next_available_at - time.monotonic())

    async def wait(self) -> float:
        remaining = self.time_until_ready()
        if remaining > 0:
            await asyncio.sleep(remaining)
        return remaining

    def schedule_next(self):
        self.next_available_at = time.monotonic() + self.sleep_time

    def record(self, total_time: float):
        self.samples.append(total_time)

        if len(self.samples) >= 5:
            avg = sum(self.samples) / len(self.samples)

            difference = self.target - avg

            self.sleep_time += difference

            self.sleep_time = max(
                self.min_sleep,
                min(self.max_sleep, self.sleep_time)
            )

            log.info(
                "avg interval %.2fs | target %.2fs | new sleep %.2fs",
                avg,
                self.target,
                self.sleep_time,
            )

            self.samples.clear()

        self.next_available_at = time.monotonic() + self.sleep_time