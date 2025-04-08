import importlib
from typing import Any, Callable, Literal, TypedDict

from pydantic import TypeAdapter
from pytest_snapshot.plugin import Snapshot

from tests.fixtures.codegen_snapshot_fixtures import validate_codegen
from tests.v1.codegen.snapshot.parity.gen import (
    gen_bool,
    gen_choice,
    gen_dict,
    gen_int,
    gen_list,
    gen_opt,
    gen_str,
)

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


def baseTestPattern[A](
    x: A, encode: Callable[[A], Any], adapter: TypeAdapter[Any]
) -> None:
    a = encode(x)
    m = adapter.validate_python(a)
    z = adapter.dump_python(m, by_alias=True, exclude_none=True)

    assert deep_equal(a, z)


def test_AiexecExecInit(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/typeddict_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=True,
    )

    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/pydantic_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=False,
    )

    import tests.v1.codegen.snapshot.snapshots.parity

    importlib.reload(tests.v1.codegen.snapshot.snapshots.parity)

    from tests.v1.codegen.snapshot.snapshots.parity import pydantic_inputs as pyd
    from tests.v1.codegen.snapshot.snapshots.parity import typeddict_inputs as tyd

    x: tyd.aiExec.ExecInit = {
        "args": gen_list(gen_str)(),
        "env": gen_opt(gen_dict(gen_str))(),
        "cwd": gen_opt(gen_str)(),
        "omitStdout": gen_opt(gen_bool)(),
        "omitStderr": gen_opt(gen_bool)(),
        "useReplitRunEnv": gen_opt(gen_bool)(),
    }

    baseTestPattern(x, tyd.aiExec.encode_ExecInit, pyd.aiExec.ExecInitTypeAdapter)


def test_AgenttoollanguageserverOpendocumentInput(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/typeddict_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=True,
    )

    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/pydantic_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=False,
    )

    import tests.v1.codegen.snapshot.snapshots.parity

    importlib.reload(tests.v1.codegen.snapshot.snapshots.parity)

    from tests.v1.codegen.snapshot.snapshots.parity import pydantic_inputs as pyd
    from tests.v1.codegen.snapshot.snapshots.parity import typeddict_inputs as tyd

    x: tyd.agentToolLanguageServer.OpendocumentInput = {
        "path": gen_str(),
    }

    baseTestPattern(
        x,
        tyd.agentToolLanguageServer.encode_OpendocumentInput,
        pyd.agentToolLanguageServer.OpendocumentInputTypeAdapter,
    )


kind_type = (
    Literal[
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
    ]
    | None
)


class size_type(TypedDict):
    rows: int
    cols: int


def test_ShellexecSpawnInput(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/typeddict_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=True,
    )

    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/pydantic_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=False,
    )

    import tests.v1.codegen.snapshot.snapshots.parity

    importlib.reload(tests.v1.codegen.snapshot.snapshots.parity)

    from tests.v1.codegen.snapshot.snapshots.parity import pydantic_inputs as pyd
    from tests.v1.codegen.snapshot.snapshots.parity import typeddict_inputs as tyd

    x: tyd.shellExec.SpawnInput = {
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
        "interactive": gen_opt(gen_bool)(),
    }

    baseTestPattern(
        x,
        tyd.shellExec.encode_SpawnInput,
        pyd.shellExec.SpawnInputTypeAdapter,
    )


def test_ConmanfilesystemPersistInput(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/typeddict_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=True,
    )

    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/pydantic_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=False,
    )

    import tests.v1.codegen.snapshot.snapshots.parity

    importlib.reload(tests.v1.codegen.snapshot.snapshots.parity)

    from tests.v1.codegen.snapshot.snapshots.parity import pydantic_inputs as pyd
    from tests.v1.codegen.snapshot.snapshots.parity import typeddict_inputs as tyd

    x: tyd.conmanFilesystem.PersistInput = {}

    baseTestPattern(
        x,
        tyd.conmanFilesystem.encode_PersistInput,
        pyd.conmanFilesystem.PersistInputTypeAdapter,
    )


def test_ReplspaceapiInitInput(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/typeddict_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=True,
    )

    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/snapshot/parity/schema.json"),
        target_path="parity/pydantic_inputs",
        client_name="foo",
        protocol_version="v1.1",
        typeddict_inputs=False,
    )

    import tests.v1.codegen.snapshot.snapshots.parity

    importlib.reload(tests.v1.codegen.snapshot.snapshots.parity)

    from tests.v1.codegen.snapshot.snapshots.parity import pydantic_inputs as pyd
    from tests.v1.codegen.snapshot.snapshots.parity import typeddict_inputs as tyd

    x: tyd.replspaceApi.init.InitInput = gen_choice(
        list[tyd.replspaceApi.init.InitInput](
            [
                tyd.replspaceApi.init.InitInputOneOf_closeFile(
                    {"kind": "closeFile", "filename": gen_str(), "nonce": gen_str()}
                ),
                tyd.replspaceApi.init.InitInputOneOf_githubToken(
                    {"kind": "githubToken", "token": gen_str(), "nonce": gen_str()}
                ),
                tyd.replspaceApi.init.InitInputOneOf_sshToken0(
                    {
                        "kind": "sshToken",
                        "nonce": gen_str(),
                        "SSHHostname": gen_str(),
                        "token": gen_str(),
                    }
                ),
                tyd.replspaceApi.init.InitInputOneOf_sshToken1(
                    {"kind": "sshToken", "nonce": gen_str(), "error": gen_str()}
                ),
                tyd.replspaceApi.init.InitInputOneOf_allowDefaultBucketAccess(
                    {
                        "kind": "allowDefaultBucketAccess",
                        "nonce": gen_str(),
                        "result": gen_choice(
                            list[
                                tyd.replspaceApi.init.InitInputOneOf_allowDefaultBucketAccessResult
                            ](
                                [
                                    tyd.replspaceApi.init.InitInputOneOf_allowDefaultBucketAccessResultOneOf_ok(
                                        {
                                            "bucketId": gen_str(),
                                            "sourceReplId": gen_str(),
                                            "status": "ok",
                                            "targetReplId": gen_str(),
                                        }
                                    ),
                                    tyd.replspaceApi.init.InitInputOneOf_allowDefaultBucketAccessResultOneOf_error(
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
        tyd.replspaceApi.init.encode_InitInput,
        pyd.replspaceApi.init.InitInputTypeAdapter,
    )
