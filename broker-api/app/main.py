from __future__ import annotations

from datetime import UTC, datetime
from functools import lru_cache
from typing import Annotated, Literal
from uuid import UUID

import boto3
import hashlib
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import psycopg
from psycopg.rows import dict_row

Classification = Literal["public", "restricted", "confidential"]
AccessReason = Literal[
    "public",
    "token_granted",
    "token_required",
    "confidential_no_download",
]


class Settings(BaseSettings):
    broker_host: str = "0.0.0.0"
    broker_port: int = 8000
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_password: str
    minio_endpoint: str
    minio_public_endpoint: str | None = None
    minio_region: str = "us-east-1"
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool = False
    presigned_url_ttl_seconds: int = 300

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class AuthContext(BaseModel):
    token_id: UUID
    label: str
    expires_at: datetime | None = None


class TokenGrant(BaseModel):
    dataset_id: UUID | None = None
    classification: Classification | None = None
    bucket: str | None = None
    key_prefix: str | None = None


class DatasetRecord(BaseModel):
    id: UUID
    slug: str
    title: str
    summary: str | None = None
    classification: Classification
    visibility: Literal["listed", "hidden"]
    storage_bucket: str
    storage_key: str
    direct_public_url: str | None = None
    mime_type: str | None = None
    file_size_bytes: int | None = None
    checksum_sha256: str | None = None
    published_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)


class CatalogItem(BaseModel):
    id: UUID
    slug: str
    title: str
    summary: str | None = None
    classification: Classification
    listed: bool = True
    downloadable: bool
    access_reason: AccessReason
    tags: list[str] = Field(default_factory=list)
    file_size_bytes: int | None = None
    mime_type: str | None = None
    published_at: datetime | None = None


class CatalogResponse(BaseModel):
    items: list[CatalogItem]


class DownloadRequest(BaseModel):
    dataset_id: UUID
    download_filename: str | None = None


class DownloadResponse(BaseModel):
    dataset_id: UUID
    allowed: bool
    reason: AccessReason
    download_url: str | None = None
    expires_in: int | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()


def build_s3_client(endpoint_url: str) -> BaseClient:
    settings = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=settings.minio_region,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        use_ssl=settings.minio_secure,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


@lru_cache
def get_s3_client() -> BaseClient:
    return build_s3_client(get_settings().minio_endpoint)


@lru_cache
def get_presign_s3_client() -> BaseClient:
    settings = get_settings()
    return build_s3_client(settings.minio_public_endpoint or settings.minio_endpoint)


def utc_now() -> datetime:
    return datetime.now(UTC)


def fingerprint_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def get_db_connection(
    settings: Settings = Depends(get_settings),
):
    with psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        row_factory=dict_row,
    ) as connection:
        yield connection


def load_auth_context(
    x_access_token: Annotated[str | None, Header()] = None,
    connection=Depends(get_db_connection),
) -> AuthContext | None:
    if not x_access_token:
        return None

    token_hash = fingerprint_token(x_access_token)
    row = connection.execute(
        """
        select id, label, expires_at
        from access_tokens
        where token_hash = %(token_hash)s
          and status = 'active'
          and (expires_at is null or expires_at > now())
        """,
        {"token_hash": token_hash},
    ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token.",
        )

    return AuthContext.model_validate(row)


def load_token_grants(connection, token_id: UUID | None) -> list[TokenGrant]:
    if not token_id:
        return []

    rows = connection.execute(
        """
        select dataset_id, classification, bucket, key_prefix
        from token_grants
        where token_id = %(token_id)s and effect = 'allow'
        order by id
        """,
        {"token_id": token_id},
    ).fetchall()
    return [TokenGrant.model_validate(row) for row in rows]


def load_catalog_dataset_rows(connection) -> list[DatasetRecord]:
    rows = connection.execute(
        """
        select
          d.id,
          d.slug,
          d.title,
          d.summary,
          d.classification,
          d.visibility,
          d.storage_bucket,
          d.storage_key,
          d.direct_public_url,
          d.mime_type,
          d.file_size_bytes,
          d.checksum_sha256,
          d.published_at,
          coalesce(array_remove(array_agg(distinct t.tag), null), '{}') as tags
        from datasets d
        left join dataset_tags t on t.dataset_id = d.id
        where d.visibility = 'listed'
        group by d.id
        order by d.published_at desc nulls last, d.title asc
        """
    ).fetchall()
    return [DatasetRecord.model_validate(row) for row in rows]


def load_dataset_by_id(connection, dataset_id: UUID) -> DatasetRecord | None:
    row = connection.execute(
        """
        select
          d.id,
          d.slug,
          d.title,
          d.summary,
          d.classification,
          d.visibility,
          d.storage_bucket,
          d.storage_key,
          d.direct_public_url,
          d.mime_type,
          d.file_size_bytes,
          d.checksum_sha256,
          d.published_at,
          coalesce(array_remove(array_agg(distinct t.tag), null), '{}') as tags
        from datasets d
        left join dataset_tags t on t.dataset_id = d.id
        where d.id = %(dataset_id)s and d.visibility = 'listed'
        group by d.id
        """,
        {"dataset_id": str(dataset_id)},
    ).fetchone()
    if not row:
        return None
    return DatasetRecord.model_validate(row)


def token_grants_dataset(dataset: DatasetRecord, grants: list[TokenGrant]) -> bool:
    for grant in grants:
        if grant.dataset_id and grant.dataset_id == dataset.id:
            return True

        if grant.classification and grant.classification == dataset.classification:
            return True

        if not grant.bucket or grant.bucket != dataset.storage_bucket:
            continue

        prefix = (grant.key_prefix or "").lstrip("/")
        if prefix == "":
            return True
        if dataset.storage_key == prefix or dataset.storage_key.startswith(
            prefix.rstrip("/") + "/"
        ):
            return True

    return False


def evaluate_dataset_access(
    dataset: DatasetRecord, grants: list[TokenGrant]
) -> tuple[bool, AccessReason]:
    if dataset.classification == "public":
        return True, "public"
    if dataset.classification == "confidential":
        return False, "confidential_no_download"
    if token_grants_dataset(dataset, grants):
        return True, "token_granted"
    return False, "token_required"


def build_catalog_item(
    dataset: DatasetRecord, grants: list[TokenGrant]
) -> CatalogItem:
    downloadable, access_reason = evaluate_dataset_access(dataset, grants)
    return CatalogItem(
        id=dataset.id,
        slug=dataset.slug,
        title=dataset.title,
        summary=dataset.summary,
        classification=dataset.classification,
        listed=dataset.visibility == "listed",
        downloadable=downloadable,
        access_reason=access_reason,
        tags=dataset.tags,
        file_size_bytes=dataset.file_size_bytes,
        mime_type=dataset.mime_type,
        published_at=dataset.published_at,
    )


def generate_download_url(
    dataset: DatasetRecord,
    s3_client: BaseClient,
    presign_client: BaseClient,
    settings: Settings,
    download_filename: str | None,
) -> tuple[str, int | None]:
    if dataset.direct_public_url:
        return dataset.direct_public_url, None

    try:
        s3_client.head_object(Bucket=dataset.storage_bucket, Key=dataset.storage_key)
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

    params = {"Bucket": dataset.storage_bucket, "Key": dataset.storage_key}
    if download_filename:
        params["ResponseContentDisposition"] = (
            f'attachment; filename="{download_filename}"'
        )

    try:
        download_url = presign_client.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=settings.presigned_url_ttl_seconds,
        )
    except ClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to create presigned URL.",
        ) from exc

    return download_url, settings.presigned_url_ttl_seconds


app = FastAPI(title="Real Good Research Broker API", version="0.2.0")


@app.get("/healthz")
def healthcheck(connection=Depends(get_db_connection)) -> dict[str, str]:
    connection.execute("select 1")
    return {"status": "ok"}


@app.get("/v1/catalog", response_model=CatalogResponse)
def catalog(
    auth_context: AuthContext | None = Depends(load_auth_context),
    connection=Depends(get_db_connection),
) -> CatalogResponse:
    grants = load_token_grants(connection, auth_context.token_id if auth_context else None)
    datasets = load_catalog_dataset_rows(connection)
    return CatalogResponse(
        items=[build_catalog_item(dataset, grants) for dataset in datasets]
    )


@app.get("/v1/datasets/{slug}", response_model=CatalogItem)
def dataset_detail(
    slug: str,
    auth_context: AuthContext | None = Depends(load_auth_context),
    connection=Depends(get_db_connection),
) -> CatalogItem:
    row = connection.execute(
        """
        select
          d.id,
          d.slug,
          d.title,
          d.summary,
          d.classification,
          d.visibility,
          d.storage_bucket,
          d.storage_key,
          d.direct_public_url,
          d.mime_type,
          d.file_size_bytes,
          d.checksum_sha256,
          d.published_at,
          coalesce(array_remove(array_agg(distinct t.tag), null), '{}') as tags
        from datasets d
        left join dataset_tags t on t.dataset_id = d.id
        where d.slug = %(slug)s and d.visibility = 'listed'
        group by d.id
        """,
        {"slug": slug},
    ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found.",
        )

    dataset = DatasetRecord.model_validate(row)
    grants = load_token_grants(connection, auth_context.token_id if auth_context else None)
    return build_catalog_item(dataset, grants)


@app.post("/v1/download-url", response_model=DownloadResponse)
def create_download_url(
    payload: DownloadRequest,
    auth_context: AuthContext | None = Depends(load_auth_context),
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    presign_client: BaseClient = Depends(get_presign_s3_client),
    settings: Settings = Depends(get_settings),
) -> DownloadResponse:
    dataset = load_dataset_by_id(connection, payload.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found.",
        )

    grants = load_token_grants(connection, auth_context.token_id if auth_context else None)
    allowed, reason = evaluate_dataset_access(dataset, grants)
    if not allowed:
        return DownloadResponse(
            dataset_id=dataset.id,
            allowed=False,
            reason=reason,
            download_url=None,
            expires_in=None,
        )

    download_url, expires_in = generate_download_url(
        dataset=dataset,
        s3_client=s3_client,
        presign_client=presign_client,
        settings=settings,
        download_filename=payload.download_filename,
    )
    return DownloadResponse(
        dataset_id=dataset.id,
        allowed=True,
        reason=reason,
        download_url=download_url,
        expires_in=expires_in,
    )
