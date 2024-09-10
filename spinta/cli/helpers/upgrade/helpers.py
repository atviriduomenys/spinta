def script_check_status_message(
    script_name: str,
    status: str
) -> str:
    return f"Script '{script_name}' upgrade check. Status: {status}"


def script_destructive_warning(
    script_name: str,
    message: str
) -> str:
    return f"WARNING (DESTRUCTIVE MODE). Script '{script_name}' will {message}."
