from __future__ import annotations


def build_planner_prompt(user_request: str) -> str:
    return (
        "You are the main planner agent.\n"
        "Plan the coding task, decide if it should be delegated to the isolated code executor, "
        "and keep the plan concise and execution-focused.\n\n"
        f"User request:\n{user_request}"
    )


def build_executor_prompt(execution_plan: str, user_request: str) -> str:
    return (
        "You are the isolated code executor agent.\n"
        "Only execute the scoped coding instruction provided below.\n"
        "Do not plan the overall workflow and do not assume access to planner-only context.\n\n"
        f"Execution plan:\n{execution_plan}\n\n"
        f"Scoped user request:\n{user_request}"
    )


def build_analyst_prompt(execution_plan: str, user_request: str) -> str:
    return (
        "You are the delegated analyst sub-agent.\n"
        "Review the request, identify what you still need from the supervisor, "
        "and ask only for the minimum missing direction before continuing.\n\n"
        f"Execution plan:\n{execution_plan}\n\n"
        f"Initial user request:\n{user_request}"
    )


def build_sub_agent_followup_prompt(execution_plan: str, user_request: str) -> str:
    return (
        "Supervisor follow-up:\n"
        "Proceed with the analysis and return a concise delegated result.\n\n"
        f"Original user request:\n{user_request}\n\n"
        f"Execution plan:\n{execution_plan}\n\n"
        "Expected output:\n"
        "- a short analysis summary\n"
        "- recommended next step for the supervisor\n"
        "- any assumptions you made"
    )
