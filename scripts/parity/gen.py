import random
from typing import Callable, Optional, TypeVar

A = TypeVar("A")


def gen_char() -> str:
    pos = random.randint(0, 26 * 2 - 1)
    if pos < 26:
        return chr(ord("A") + pos)
    else:
        return chr(ord("a") + pos - 26)


def gen_str() -> str:
    return "".join(gen_char() for _ in range(0, random.randint(0, 20)))


def gen_list(gen_x: Callable[[], A]) -> Callable[[], list[A]]:
    return lambda: [gen_x() for _ in range(0, random.randint(0, 10))]


def gen_bool() -> bool:
    # Ten times more likely to be true than false
    return bool(random.randint(0, 10))


def gen_float() -> float:
    return random.random() * 100


def gen_int() -> int:
    return random.randint(0, 2048)


def gen_choice(choices: list[A]) -> Callable[[], A]:
    return lambda: random.choice(choices)


def gen_opt(gen_x: Callable[[], A]) -> Callable[[], Optional[A]]:
    return lambda: gen_x() if gen_bool() else None


def gen_dict(gen_x: Callable[[], A]) -> Callable[[], dict[str, A]]:
    return lambda: dict((gen_str(), gen_x()) for _ in range(0, random.randint(0, 5)))
