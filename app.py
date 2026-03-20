import logging

from boxer_adapter_slack.runtime import main as slack_main


def main() -> None:
    # Legacy entrypoint kept for existing systemd units and local workflows.
    logging.basicConfig(level=logging.INFO)
    slack_main()


if __name__ == "__main__":
    main()
