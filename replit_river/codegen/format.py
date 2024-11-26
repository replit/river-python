from textwrap import dedent, indent


def reindent(prefix: str, code: str) -> str:
    """
    Take an arbitrarily indented code block, dedent to the lowest common
    indent level and then reindent based on the supplied prefix
    """
    return indent(dedent(code.rstrip()), prefix)
