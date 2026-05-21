from __future__ import annotations

import argparse

from shared.auth import ServiceTokenIssuer


def main() -> None:
    parser = argparse.ArgumentParser(description="Mint a local dev JWT for the A2A demo.")
    parser.add_argument("--aud", default="planner-service", help="JWT audience")
    parser.add_argument("--sub", default="local-client", help="JWT subject")
    parser.add_argument("--iss", default="a2a-local-auth", help="JWT issuer")
    parser.add_argument("--kid", default="planner-local-dev-key", help="JWT key id")
    parser.add_argument("--key", default="dev_auth/private_key.pem", help="Path to signing private key")
    parser.add_argument("--ttl", type=int, default=300, help="Token lifetime in seconds")
    args = parser.parse_args()

    issuer = ServiceTokenIssuer(args.key, args.iss, args.kid, ttl_seconds=args.ttl)
    print(issuer.issue_token(args.aud, args.sub))


if __name__ == "__main__":
    main()
