from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives.serialization import load_pem_private_key


class BearerTokenValidationError(RuntimeError):
    pass


def extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise BearerTokenValidationError("Missing Authorization header.")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise BearerTokenValidationError("Expected a Bearer token.")
    return token


def _base64url_uint(value: int) -> str:
    width = max(1, (value.bit_length() + 7) // 8)
    raw = value.to_bytes(width, byteorder="big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def build_local_jwks(private_key_path: str, kid: str) -> dict[str, Any]:
    private_key = load_pem_private_key(Path(private_key_path).read_bytes(), password=None)
    public_numbers = private_key.public_key().public_numbers()
    return {
        "keys": [
            {
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": kid,
                "n": _base64url_uint(public_numbers.n),
                "e": _base64url_uint(public_numbers.e),
            }
        ]
    }


class JWKSBearerValidator:
    def __init__(
        self,
        jwks_url: str,
        issuer: str,
        audience: str,
        *,
        algorithms: tuple[str, ...] = ("RS256",),
        cache_ttl_seconds: int = 300,
        timeout: float = 5.0,
    ) -> None:
        self.jwks_url = jwks_url
        self.issuer = issuer
        self.audience = audience
        self.algorithms = algorithms
        self.cache_ttl_seconds = cache_ttl_seconds
        self.timeout = timeout
        self._cached_jwks: dict[str, Any] | None = None
        self._cache_deadline = 0.0

    async def validate(self, token: str) -> dict[str, Any]:
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as exc:
            raise BearerTokenValidationError("Invalid token header.") from exc

        kid = header.get("kid")
        alg = header.get("alg")
        if alg not in self.algorithms:
            raise BearerTokenValidationError("Unsupported token algorithm.")
        if not kid:
            raise BearerTokenValidationError("Missing token key id.")

        key = await self._resolve_signing_key(kid)
        try:
            claims = jwt.decode(
                token,
                key=key,
                algorithms=list(self.algorithms),
                audience=self.audience,
                issuer=self.issuer,
            )
        except jwt.PyJWTError as exc:
            raise BearerTokenValidationError("Bearer token validation failed.") from exc
        return dict(claims)

    async def _resolve_signing_key(self, kid: str) -> Any:
        jwks = await self._get_jwks()
        key = self._select_key(jwks, kid)
        if key is None:
            jwks = await self._get_jwks(force_refresh=True)
            key = self._select_key(jwks, kid)
        if key is None:
            raise BearerTokenValidationError("Unknown token signing key.")
        return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key))

    async def _get_jwks(self, *, force_refresh: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        if not force_refresh and self._cached_jwks is not None and now < self._cache_deadline:
            return self._cached_jwks

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(self.jwks_url)
            response.raise_for_status()
            jwks = response.json()

        self._cached_jwks = jwks
        self._cache_deadline = now + self.cache_ttl_seconds
        return jwks

    @staticmethod
    def _select_key(jwks: dict[str, Any], kid: str) -> dict[str, Any] | None:
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                return key
        return None


class ServiceTokenIssuer:
    def __init__(
        self,
        private_key_path: str,
        issuer: str,
        kid: str,
        *,
        ttl_seconds: int = 300,
        algorithm: str = "RS256",
    ) -> None:
        self.private_key = Path(private_key_path).read_text()
        self.issuer = issuer
        self.kid = kid
        self.ttl_seconds = ttl_seconds
        self.algorithm = algorithm

    def issue_token(self, audience: str, subject: str, extra_claims: dict[str, Any] | None = None) -> str:
        now = int(time.time())
        payload: dict[str, Any] = {
            "iss": self.issuer,
            "sub": subject,
            "aud": audience,
            "iat": now,
            "nbf": now,
            "exp": now + self.ttl_seconds,
        }
        if extra_claims:
            payload.update(extra_claims)
        return jwt.encode(
            payload,
            self.private_key,
            algorithm=self.algorithm,
            headers={"kid": self.kid},
        )
