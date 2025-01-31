from dataclasses import dataclass
from typing import NewType, assert_never

TypeName = NewType("TypeName", str)
ModuleName = NewType("ModuleName", str)
ClassName = NewType("ClassName", str)
FileContents = NewType("FileContents", str)
HandshakeType = NewType("HandshakeType", str)

RenderedPath = NewType("RenderedPath", str)


@dataclass
class DictTypeExpr:
    nested: "TypeExpression"


@dataclass
class ListTypeExpr:
    nested: "TypeExpression"


@dataclass
class LiteralTypeExpr:
    nested: int | str


@dataclass
class UnionTypeExpr:
    nested: list["TypeExpression"]


@dataclass
class UnknownTypeExpr:
    name: TypeName


TypeExpression = (
    TypeName
    | DictTypeExpr
    | ListTypeExpr
    | LiteralTypeExpr
    | UnionTypeExpr
    | UnknownTypeExpr
)


def render_type_expr(value: TypeExpression) -> str:
    match value:
        case DictTypeExpr(nested):
            return f"dict[str, {render_type_expr(nested)}]"
        case ListTypeExpr(nested):
            return f"list[{render_type_expr(nested)}]"
        case LiteralTypeExpr(inner):
            return f"Literal[{repr(inner)}]"
        case UnionTypeExpr(inner):
            return " | ".join(render_type_expr(x) for x in inner)
        case str(name):
            return TypeName(name)
        case UnknownTypeExpr(name):
            return TypeName(name)
        case other:
            assert_never(other)


def extract_inner_type(value: TypeExpression) -> TypeName:
    match value:
        case DictTypeExpr(nested):
            return extract_inner_type(nested)
        case ListTypeExpr(nested):
            return extract_inner_type(nested)
        case LiteralTypeExpr(_):
            raise ValueError(f"Unexpected literal type: {value}")
        case UnionTypeExpr(_):
            raise ValueError(
                f"Attempting to extract from a union, currently not possible: {value}"
            )
        case str(name):
            return TypeName(name)
        case UnknownTypeExpr(name):
            return name
        case other:
            assert_never(other)


def ensure_literal_type(value: TypeExpression) -> TypeName:
    match value:
        case DictTypeExpr(_):
            raise ValueError(
                f"Unexpected expression when expecting a type name: {value}"
            )
        case ListTypeExpr(_):
            raise ValueError(
                f"Unexpected expression when expecting a type name: {value}"
            )
        case LiteralTypeExpr(_):
            raise ValueError(
                f"Unexpected expression when expecting a type name: {value}"
            )
        case UnionTypeExpr(_):
            raise ValueError(
                f"Unexpected expression when expecting a type name: {value}"
            )
        case str(name):
            return TypeName(name)
        case UnknownTypeExpr(name):
            return name
        case other:
            assert_never(other)
