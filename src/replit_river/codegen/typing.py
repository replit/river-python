from dataclasses import dataclass
from typing import NewType, assert_never

ModuleName = NewType("ModuleName", str)
ClassName = NewType("ClassName", str)
FileContents = NewType("FileContents", str)
HandshakeType = NewType("HandshakeType", str)

RenderedPath = NewType("RenderedPath", str)


@dataclass(frozen=True)
class TypeName:
    value: str

    def __str__(self) -> str:
        raise Exception("Complex type must be put through render_type_expr!")


@dataclass(frozen=True)
class NoneTypeExpr:
    def __str__(self) -> str:
        raise Exception("Complex type must be put through render_type_expr!")


@dataclass(frozen=True)
class DictTypeExpr:
    nested: "TypeExpression"

    def __str__(self) -> str:
        raise Exception("Complex type must be put through render_type_expr!")


@dataclass(frozen=True)
class ListTypeExpr:
    nested: "TypeExpression"

    def __str__(self) -> str:
        raise Exception("Complex type must be put through render_type_expr!")


@dataclass(frozen=True)
class LiteralTypeExpr:
    nested: int | str

    def __str__(self) -> str:
        raise Exception("Complex type must be put through render_type_expr!")


@dataclass(frozen=True)
class UnionTypeExpr:
    nested: list["TypeExpression"]

    def __str__(self) -> str:
        raise Exception("Complex type must be put through render_type_expr!")


@dataclass(frozen=True)
class OpenUnionTypeExpr:
    union: UnionTypeExpr

    def __str__(self) -> str:
        raise Exception("Complex type must be put through render_type_expr!")


TypeExpression = (
    TypeName
    | NoneTypeExpr
    | DictTypeExpr
    | ListTypeExpr
    | LiteralTypeExpr
    | UnionTypeExpr
    | OpenUnionTypeExpr
)


def _flatten_nested_unions(value: TypeExpression) -> TypeExpression:
    def work(
        value: TypeExpression,
    ) -> tuple[list[TypeExpression], TypeExpression | None]:
        match value:
            case UnionTypeExpr(inner):
                flattened: list[TypeExpression] = []
                for tpe in inner:
                    _union, _nonunion = work(tpe)
                    flattened.extend(_union)
                    if _nonunion is not None:
                        flattened.append(_nonunion)
                return (flattened, None)
            case other:
                return ([], other)

    _inner, nonunion = work(value)
    if nonunion and not _inner:
        return nonunion
    elif _inner and nonunion is None:
        return UnionTypeExpr(_inner)
    else:
        raise ValueError("Incoherent state when trying to flatten unions")


def render_type_expr(value: TypeExpression) -> str:
    match _flatten_nested_unions(value):
        case DictTypeExpr(nested):
            return f"dict[str, {render_type_expr(nested)}]"
        case ListTypeExpr(nested):
            return f"list[{render_type_expr(nested)}]"
        case LiteralTypeExpr(inner):
            return f"Literal[{repr(inner)}]"
        case UnionTypeExpr(inner):
            literals: list[LiteralTypeExpr] = []
            _other: list[TypeExpression] = []
            for tpe in inner:
                if isinstance(tpe, UnionTypeExpr):
                    raise ValueError("These should have been flattened")
                elif isinstance(tpe, LiteralTypeExpr):
                    literals.append(tpe)
                else:
                    _other.append(tpe)
            retval: str = " | ".join(render_type_expr(x) for x in _other)
            if literals:
                _rendered: str = ", ".join(repr(x.nested) for x in literals)
                if retval:
                    retval = f"Literal[{_rendered}] | {retval}"
                else:
                    retval = f"Literal[{_rendered}]"
            return retval
        case OpenUnionTypeExpr(inner):
            return (
                "Annotated["
                f"{render_type_expr(inner)} | RiverUnknownValue,"
                "WrapValidator(translate_unknown_value)"
                "]"
            )
        case TypeName(name):
            return name
        case NoneTypeExpr():
            return "None"
        case other:
            assert_never(other)


def render_literal_type(value: TypeExpression) -> str:
    return render_type_expr(ensure_literal_type(value))


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
        case OpenUnionTypeExpr(_):
            raise ValueError(
                f"Attempting to extract from a union, currently not possible: {value}"
            )
        case TypeName(name):
            return TypeName(name)
        case NoneTypeExpr():
            raise ValueError(f"Attempting to extract from a literal 'None': {value}")
        case other:
            assert_never(other)


def ensure_literal_type(value: TypeExpression) -> TypeName:
    match value:
        case TypeName(name):
            return TypeName(name)
        case other:
            raise ValueError(
                f"Unexpected expression when expecting a type name: {other}"
            )
