
import asyncio
import logging
log = logging.getLogger(__name__)


class AdaptiveDelay:
    def __init__(self, target: float, initial_sleep: float):
        self.target = target
        self.sleep_time = initial_sleep
        self.samples = []

    async def wait(self):
        await asyncio.sleep(self.sleep_time)

    def record(self, total_time: float):
        self.samples.append(total_time)

        if len(self.samples) >= 5:
            avg = sum(self.samples) / len(self.samples)

            difference = self.target - avg

            self.sleep_time += difference

            self.sleep_time = max(
                0,
                min(60, self.sleep_time)
            )

            log.info(
                "avg interval %.2fs | target %.2fs | new sleep %.2fs",
                avg,
                self.target,
                self.sleep_time,
            )

            self.samples.clear()