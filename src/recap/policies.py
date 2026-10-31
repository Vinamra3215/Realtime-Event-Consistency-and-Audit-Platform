"""Pure recovery schedules. Attempt 1 is initial submit; first retry uses n=0."""

import random
from dataclasses import dataclass

from recap.provenance import stable_hash


@dataclass(frozen=True)
class Policy:
    name: str
    check_first: bool = False
    exponential: bool = False
    jitter: bool = False

    def delay(self, completed_attempts, settings, identity):
        n = max(0, completed_attempts - 1)
        ceiling = (
            min(settings.base_delay * 2 ** min(n, 30), settings.max_delay)
            if self.exponential
            else settings.base_delay
        )
        if self.jitter:
            return random.Random(stable_hash([settings.seed, identity, n, "retry-jitter"])).uniform(
                0, ceiling
            )
        return ceiling


POLICIES = {
    "fixed": Policy("fixed"),
    "exponential": Policy("exponential", exponential=True),
    "jitter": Policy("jitter", exponential=True, jitter=True),
    "status_check_first": Policy("status_check_first", check_first=True, exponential=True),
}


def get_policy(name):
    try:
        return POLICIES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown recovery policy {name}") from exc
