from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from functools import lru_cache
from typing import Annotated, Any

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    broker_host: str = "0.0.0.0"
    broker_port: int = 8000
    minio_endpoint: str
    minio_region: str = "us-east-1"
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool = False
    presigned_url_ttl_seconds: int = 300
    token_config_path: str = "/app/config/tokens.json"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class AccessRule(BaseModel):
    bucket: str = Field(min_length=3)
    prefix: str = ""


class TokenPolicy(BaseModel):
    label: str
    expires_at: datetime | None = None
    allow: list[AccessRule] = Field(default_factory=list)


class TokenConfig(BaseModel):
    tokens: dict[str, TokenPolicy]


class DownloadRequest(BaseModel):
    bucket: str
    object_key: str = Field(min_length=1)
    download_filename: str | None = None


class DownloadResponse(BaseModel):
    bucket: str
    object_key: str
    expires_in: int
    download_url: str


class CatalogEntry(BaseModel):
    bucket: str
    prefix: str


class CatalogResponse(BaseModel):
    token_label: str
    expires_at: datetime | None
    allow: list[CatalogEntry]


@lru_cache
def get_settings() -> Settings:
    return Settings()


@lru_cache
def get_s3_client() -> BaseClient:
    settings = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        region_name=settings.minio_region,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        use_ssl=settings.minio_secure,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_object_key(object_key: str) -> str:
    if "\\" in object_key:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid object key.",
        )

    parts = [part for part in object_key.split("/") if part not in {"", "."}]
    if any(part == ".." for part in parts):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid object key.",
        )

    normalized = "/".join(parts)
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid object key.",
        )
    return normalized


def load_token_config() -> TokenConfig:
    settings = get_settings()
    try:
        with open(settings.token_config_path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Token config file not found: {settings.token_config_path}"
        ) from exc

    return TokenConfig.model_validate(raw)


def fingerprint_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def get_policy_by_token(access_token: str) -> TokenPolicy:
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing access token.",
        )

    token_config = load_token_config()
    candidate_fingerprint = fingerprint_token(access_token)

    for raw_token, policy in token_config.tokens.items():
        if hmac.compare_digest(candidate_fingerprint, fingerprint_token(raw_token)):
            if policy.expires_at and policy.expires_at <= utc_now():
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access token expired.",
                )
            return policy

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid access token.",
    )


def is_allowed(policy: TokenPolicy, bucket: str, object_key: str) -> bool:
    for rule in policy.allow:
        prefix = rule.prefix.lstrip("/")
        if bucket != rule.bucket:
            continue
        if prefix == "":
            return True
        if object_key == prefix or object_key.startswith(prefix.rstrip("/") + "/"):
            return True
    return False


def get_token_policy(
    x_access_token: Annotated[str | None, Header()] = None,
) -> TokenPolicy:
    return get_policy_by_token(x_access_token or "")


app = FastAPI(title="Real Good Research Broker API", version="0.1.0")


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/catalog", response_model=CatalogResponse)
def catalog(policy: TokenPolicy = Depends(get_token_policy)) -> CatalogResponse:
    return CatalogResponse(
        token_label=policy.label,
        expires_at=policy.expires_at,
        allow=[
            CatalogEntry(bucket=rule.bucket, prefix=rule.prefix) for rule in policy.allow
        ],
    )


@app.post("/v1/download-url", response_model=DownloadResponse)
def create_download_url(
    payload: DownloadRequest,
    policy: TokenPolicy = Depends(get_token_policy),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
) -> DownloadResponse:
    object_key = normalize_object_key(payload.object_key)

    if not is_allowed(policy, payload.bucket, object_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token is not allowed to access that object.",
        )

    try:
        s3_client.head_object(Bucket=payload.bucket, Key=object_key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Requested object was not found.",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Storage backend error while validating object.",
        ) from exc

    params: dict[str, Any] = {"Bucket": payload.bucket, "Key": object_key}
    if payload.download_filename:
        params["ResponseContentDisposition"] = (
            f'attachment; filename="{payload.download_filename}"'
        )

    try:
        download_url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=settings.presigned_url_ttl_seconds,
        )
    except ClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to create presigned URL.",
        ) from exc

    return DownloadResponse(
        bucket=payload.bucket,
        object_key=object_key,
        expires_in=settings.presigned_url_ttl_seconds,
        download_url=download_url,
    )
