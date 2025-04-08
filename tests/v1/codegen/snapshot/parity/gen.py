import random
import string
from typing import Callable, TypeVar

A = TypeVar("A")


printable_chars = string.ascii_letters + string.digits


def gen_char() -> str:
    return random.choice(printable_chars)


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


def gen_opt(gen_x: Callable[[], A]) -> Callable[[], A | None]:
    return lambda: gen_x() if gen_bool() else None


def gen_dict(gen_x: Callable[[], A]) -> Callable[[], dict[str, A]]:
    return lambda: dict((gen_str(), gen_x()) for _ in range(0, random.randint(0, 5)))
