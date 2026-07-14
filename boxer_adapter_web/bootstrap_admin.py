from __future__ import annotations

import argparse

from boxer_adapter_web.auth import hash_password
from boxer_adapter_web.settings import get_web_settings
from boxer_adapter_web.storage import WebChatStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Boxer Web admin account")
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    settings = get_web_settings()
    store = WebChatStore(settings.data_path)
    store.initialize()
    admin_user = store.upsert_admin_user(
        email=args.email,
        name=args.name,
        password_hash=hash_password(args.password),
    )
    print("Admin ready")
    print(f"Email: {admin_user['email']}")
    print(f"Name: {admin_user['name']}")


if __name__ == "__main__":
    main()
