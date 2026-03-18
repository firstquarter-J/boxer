import logging

from slack_bolt.adapter.socket_mode import SocketModeHandler

from boxer.adapters.factory import create_app
from boxer.core import settings as s


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    bolt_app = create_app()
    SocketModeHandler(bolt_app, s.SLACK_APP_TOKEN).start()


if __name__ == "__main__":
    main()
