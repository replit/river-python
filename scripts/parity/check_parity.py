from typing import Any, Callable, Literal, TypedDict, TypeVar, Union, cast

import pyd
import tyd
from parity.gen import (
    gen_bool,
    gen_choice,
    gen_dict,
    gen_float,
    gen_int,
    gen_list,
    gen_opt,
    gen_str,
)
from pydantic import TypeAdapter

A = TypeVar("A")


def baseTestPattern(
    x: A, encode: Callable[[A], Any], adapter: TypeAdapter[Any]
) -> None:
    a = encode(x)
    m = adapter.validate_python(a)
    z = adapter.dump_python(m)

    assert a == z


def testAiexecExecInit() -> None:
    x: tyd.AiexecExecInit = {
        "args": gen_list(gen_str)(),
        "env": gen_opt(gen_dict(gen_str))(),
        "cwd": gen_opt(gen_str)(),
        "omitStdout": gen_opt(gen_bool)(),
        "omitStderr": gen_opt(gen_bool)(),
        "useReplitRunEnv": gen_opt(gen_bool)(),
    }

    baseTestPattern(x, tyd.encode_AiexecExecInit, TypeAdapter(pyd.AiexecExecInit))


def testAgenttoollanguageserverOpendocumentInput() -> None:
    x: tyd.AgenttoollanguageserverOpendocumentInput = {
        "uri": gen_str(),
        "languageId": gen_str(),
        "version": gen_float(),
        "text": gen_str(),
    }

    baseTestPattern(
        x,
        tyd.encode_AgenttoollanguageserverOpendocumentInput,
        TypeAdapter(pyd.AgenttoollanguageserverOpendocumentInput),
    )


kind_type = Union[
    Literal[1],
    Literal[2],
    Literal[3],
    Literal[4],
    Literal[5],
    Literal[6],
    Literal[7],
    Literal[8],
    Literal[9],
    Literal[10],
    Literal[11],
    Literal[12],
    Literal[13],
    Literal[14],
    Literal[15],
    Literal[16],
    Literal[17],
    Literal[18],
    Literal[19],
    Literal[20],
    Literal[21],
    Literal[22],
    Literal[23],
    Literal[24],
    Literal[25],
    Literal[26],
    None,
]


def testAgenttoollanguageserverGetcodesymbolInput() -> None:
    x: tyd.AgenttoollanguageserverGetcodesymbolInput = {
        "uri": gen_str(),
        "position": {
            "line": gen_float(),
            "character": gen_float(),
        },
        "kind": cast(kind_type, gen_opt(gen_choice(list(range(1, 27))))()),
    }

    baseTestPattern(
        x,
        tyd.encode_AgenttoollanguageserverGetcodesymbolInput,
        TypeAdapter(pyd.AgenttoollanguageserverGetcodesymbolInput),
    )


class size_type(TypedDict):
    rows: int
    cols: int


def testShellexecSpawnInput() -> None:
    x: tyd.ShellexecSpawnInput = {
        "cmd": gen_str(),
        "args": gen_opt(gen_list(gen_str))(),
        "initialCmd": gen_opt(gen_str)(),
        "env": gen_opt(gen_dict(gen_str))(),
        "cwd": gen_opt(gen_str)(),
        "size": gen_opt(
            lambda: cast(
                size_type,
                {
                    "rows": gen_int(),
                    "cols": gen_int(),
                },
            )
        )(),
        "useReplitRunEnv": gen_opt(gen_bool)(),
        "useCgroupMagic": gen_opt(gen_bool)(),
        "interactive": gen_opt(gen_bool)(),
    }

    baseTestPattern(
        x,
        tyd.encode_ShellexecSpawnInput,
        TypeAdapter(pyd.ShellexecSpawnInput),
    )


def testConmanfilesystemPersistInput() -> None:
    x: tyd.ConmanfilesystemPersistInput = {}

    baseTestPattern(
        x,
        tyd.encode_ConmanfilesystemPersistInput,
        TypeAdapter(pyd.ConmanfilesystemPersistInput),
    )


def main() -> None:
    testAiexecExecInit()
    testAgenttoollanguageserverOpendocumentInput()
    testAgenttoollanguageserverGetcodesymbolInput()
    testShellexecSpawnInput()
    testConmanfilesystemPersistInput()


if __name__ == "__main__":
    print("Starting...")
    for _ in range(0, 100):
        main()
    print("Verified")
