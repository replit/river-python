from typing import Any, Callable, Literal, TypedDict, TypeVar, Union

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

PrimitiveType = (
    bool | str | int | float | dict[str, "PrimitiveType"] | list["PrimitiveType"]
)


def deep_equal(a: PrimitiveType, b: PrimitiveType) -> Literal[True]:
    if a == b:
        return True
    elif isinstance(a, dict) and isinstance(b, dict):
        a_keys: PrimitiveType = list(a.keys())
        b_keys: PrimitiveType = list(b.keys())
        assert deep_equal(a_keys, b_keys)

        # We do this dance again because Python variance is hard. Feel free to fix it.
        keys = set(a.keys())
        keys.update(b.keys())
        for k in keys:
            aa: PrimitiveType = a[k]
            bb: PrimitiveType = b[k]
            assert deep_equal(aa, bb)
        return True
    elif isinstance(a, list) and isinstance(b, list):
        assert len(a) == len(b)
        for i in range(len(a)):
            assert deep_equal(a[i], b[i])
        return True
    else:
        assert a == b, f"{a} != {b}"
        return True


def baseTestPattern(
    x: A, encode: Callable[[A], Any], adapter: TypeAdapter[Any]
) -> None:
    a = encode(x)
    m = adapter.validate_python(a)
    z = adapter.dump_python(m, by_alias=True, exclude_none=True)

    assert deep_equal(a, z)


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
        "kind": gen_choice(
            list[kind_type](
                [
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    None,
                ]
            )
        )(),
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
            lambda: size_type(
                {
                    "rows": gen_int(),
                    "cols": gen_int(),
                }
            ),
        )(),
        "useReplitRunEnv": gen_opt(gen_bool)(),
        "useCgroupMagic": gen_opt(gen_bool)(),
        "interactive": gen_opt(gen_bool)(),
        "onlySpawnIfNoProcesses": gen_opt(gen_bool)(),
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


closeFile = tyd.ReplspaceapiInitInputOneOf_closeFile
githubToken = tyd.ReplspaceapiInitInputOneOf_githubToken
sshToken0 = tyd.ReplspaceapiInitInputOneOf_sshToken0
sshToken1 = tyd.ReplspaceapiInitInputOneOf_sshToken1
allowDefaultBucketAccess = tyd.ReplspaceapiInitInputOneOf_allowDefaultBucketAccess

allowDefaultBucketAccessResultOk = (
    tyd.ReplspaceapiInitInputOneOf_allowDefaultBucketAccessResultOneOf_ok
)
allowDefaultBucketAccessResultError = (
    tyd.ReplspaceapiInitInputOneOf_allowDefaultBucketAccessResultOneOf_error
)


def testReplspaceapiInitInput() -> None:
    x: tyd.ReplspaceapiInitInput = gen_choice(
        list[tyd.ReplspaceapiInitInput](
            [
                closeFile(
                    {"kind": "closeFile", "filename": gen_str(), "nonce": gen_str()}
                ),
                githubToken(
                    {"kind": "githubToken", "token": gen_str(), "nonce": gen_str()}
                ),
                sshToken0(
                    {
                        "kind": "sshToken",
                        "nonce": gen_str(),
                        "SSHHostname": gen_str(),
                        "token": gen_str(),
                    }
                ),
                sshToken1({"kind": "sshToken", "nonce": gen_str(), "error": gen_str()}),
                allowDefaultBucketAccess(
                    {
                        "kind": "allowDefaultBucketAccess",
                        "nonce": gen_str(),
                        "result": gen_choice(
                            list[
                                tyd.ReplspaceapiInitInputOneOf_allowDefaultBucketAccessResult
                            ](
                                [
                                    allowDefaultBucketAccessResultOk(
                                        {
                                            "bucketId": gen_str(),
                                            "sourceReplId": gen_str(),
                                            "status": "ok",
                                            "targetReplId": gen_str(),
                                        }
                                    ),
                                    allowDefaultBucketAccessResultError(
                                        {"message": gen_str(), "status": "error"}
                                    ),
                                ]
                            )
                        )(),
                    }
                ),
            ]
        )
    )()

    baseTestPattern(
        x,
        tyd.encode_ReplspaceapiInitInput,
        TypeAdapter(pyd.ReplspaceapiInitInput),
    )


def main() -> None:
    testAiexecExecInit()
    testAgenttoollanguageserverOpendocumentInput()
    testAgenttoollanguageserverGetcodesymbolInput()
    testShellexecSpawnInput()
    testConmanfilesystemPersistInput()
    testReplspaceapiInitInput()


if __name__ == "__main__":
    print("Starting...")
    for _ in range(0, 100):
        main()
    print("Verified")
