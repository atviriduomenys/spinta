from spinta import spyna


def binds_to_strs(ast):
    # XXX: After replacing pyrql with spinta.spyna, AST has changed and now
    #      binds are explicit and looks like this:
    #
    #           {'name': 'bind', 'args': ['name']}
    #
    #      Previously this lookes like this:
    #
    #           'name'
    #
    #      This functions goes though all ast and converts explicit  binds to
    #      strings to make AST backwards compatible.
    #
    #      But this is a temporary solution, binds must be explicit and we need
    #      to know when user passed a bind and when a literal string.

    # Return all literals as is.
    if not isinstance(ast, dict):
        return ast

    if ast['name'] in ('bind', 'getattr'):
        return spyna.unparse(ast)

    # Process all AST recursively.
    return {
        **ast,
        'args': [binds_to_strs(x) for x in ast['args']],
    }
