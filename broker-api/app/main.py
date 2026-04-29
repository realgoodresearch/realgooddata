from __future__ import annotations

import base64
from datetime import UTC, datetime
from functools import lru_cache
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
import time
from typing import Annotated, Literal
from uuid import UUID
from math import ceil

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

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
    admin_username: str
    admin_password: str
    admin_session_secret: str
    admin_session_ttl_seconds: int = 43200

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
    collection_id: UUID | None = None
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
    sort_order: int = 0
    published_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)


class CollectionRecord(BaseModel):
    id: UUID
    slug: str
    title: str
    summary: str | None = None
    readme_bucket: str | None = None
    readme_key: str | None = None
    published_at: datetime | None = None


class CatalogItem(BaseModel):
    id: UUID
    collection_id: UUID | None = None
    slug: str
    filename: str
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


class CollectionCounts(BaseModel):
    total: int
    public: int
    restricted: int
    confidential: int
    downloadable: int
    locked: int


class CollectionSummary(BaseModel):
    id: UUID
    slug: str
    title: str
    summary: str | None = None
    published_at: datetime | None = None
    counts: CollectionCounts
    search_text: str = ""


class CollectionsResponse(BaseModel):
    items: list[CollectionSummary]


class CollectionDetail(BaseModel):
    id: UUID
    slug: str
    title: str
    summary: str | None = None
    published_at: datetime | None = None
    readme_url: str | None = None
    readme_filename: str | None = None
    counts: CollectionCounts
    datasets: list[CatalogItem]


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


APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


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


def fingerprint_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def parse_optional_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_optional_uuid(value: str | None) -> UUID | None:
    if not value:
        return None
    return UUID(value)


def parse_optional_int(value: str | None) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def parse_page(value: str | None) -> int:
    try:
        page = int(value or "1")
    except ValueError:
        return 1
    return max(page, 1)


def normalize_tags(value: str | None) -> list[str]:
    if not value:
        return []
    seen: set[str] = set()
    tags: list[str] = []
    for raw_tag in value.split(","):
        tag = raw_tag.strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        tags.append(tag)
    return tags


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "dataset"


def object_title_from_key(key: str) -> str:
    filename = object_public_filename(key)
    stem, _, _extension = filename.rpartition(".")
    if not stem:
        stem = filename
    title = re.sub(r"[_-]+", " ", stem).strip()
    return title or filename


def parse_readme_selection(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    bucket, separator, key = value.partition("::")
    if not separator or not bucket or not key:
        return None, None
    return bucket, key


def build_admin_session(settings: Settings) -> str:
    payload = {
        "u": settings.admin_username,
        "exp": int(time.time()) + settings.admin_session_ttl_seconds,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode("ascii").rstrip("=")
    signature = hmac.new(
        settings.admin_session_secret.encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload_b64}.{signature}"


def verify_admin_session(session_value: str | None, settings: Settings) -> bool:
    if not session_value or "." not in session_value:
        return False
    payload_b64, signature = session_value.rsplit(".", 1)
    expected_signature = hmac.new(
        settings.admin_session_secret.encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        return False

    padding = "=" * (-len(payload_b64) % 4)
    try:
        payload = json.loads(
            base64.urlsafe_b64decode((payload_b64 + padding).encode("ascii"))
        )
    except (ValueError, json.JSONDecodeError):
        return False

    return (
        payload.get("u") == settings.admin_username
        and int(payload.get("exp", 0)) > int(time.time())
    )


def admin_authenticated(request: Request, settings: Settings) -> bool:
    return verify_admin_session(request.cookies.get("rrg_admin_session"), settings)


def admin_login_redirect() -> RedirectResponse:
    return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)


def set_admin_session_cookie(response: RedirectResponse, settings: Settings) -> None:
    response.set_cookie(
        key="rrg_admin_session",
        value=build_admin_session(settings),
        max_age=settings.admin_session_ttl_seconds,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/admin",
    )


def clear_admin_session_cookie(response: RedirectResponse) -> None:
    response.delete_cookie(key="rrg_admin_session", path="/admin")


def get_db_connection(settings: Settings = Depends(get_settings)):
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

    row = connection.execute(
        """
        select id as token_id, label, expires_at
        from access_tokens
        where token_hash = %(token_hash)s
          and status = 'active'
          and (expires_at is null or expires_at > now())
        """,
        {"token_hash": fingerprint_token(x_access_token)},
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
          d.collection_id,
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
          d.sort_order,
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
          d.collection_id,
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
          d.sort_order,
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


def load_dataset_by_slug(connection, slug: str) -> DatasetRecord | None:
    row = connection.execute(
        """
        select
          d.id,
          d.collection_id,
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
          d.sort_order,
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
        return None
    return DatasetRecord.model_validate(row)


def load_collections(connection) -> list[CollectionRecord]:
    rows = connection.execute(
        """
        select c.id, c.slug, c.title, c.summary, c.readme_bucket, c.readme_key, c.published_at
        from collections c
        where exists (
          select 1
          from datasets d
          where d.collection_id = c.id and d.visibility = 'listed'
        )
        order by c.published_at desc nulls last, c.title asc
        """
    ).fetchall()
    return [CollectionRecord.model_validate(row) for row in rows]


def load_collection_by_slug(connection, slug: str) -> CollectionRecord | None:
    row = connection.execute(
        """
        select c.id, c.slug, c.title, c.summary, c.readme_bucket, c.readme_key, c.published_at
        from collections c
        where c.slug = %(slug)s
          and exists (
            select 1
            from datasets d
            where d.collection_id = c.id and d.visibility = 'listed'
          )
        """,
        {"slug": slug},
    ).fetchone()
    if not row:
        return None
    return CollectionRecord.model_validate(row)


def load_collection_datasets(connection, collection_id: UUID) -> list[DatasetRecord]:
    rows = connection.execute(
        """
        select
          d.id,
          d.collection_id,
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
          d.sort_order,
          d.published_at,
          coalesce(array_remove(array_agg(distinct t.tag), null), '{}') as tags
        from datasets d
        left join dataset_tags t on t.dataset_id = d.id
        where d.collection_id = %(collection_id)s
          and d.visibility = 'listed'
        group by d.id
        order by d.sort_order asc, d.published_at desc nulls last, d.title asc
        """,
        {"collection_id": str(collection_id)},
    ).fetchall()
    return [DatasetRecord.model_validate(row) for row in rows]


def load_admin_collection_rows(connection) -> list[dict]:
    return connection.execute(
        """
        select
          c.id,
          c.slug,
          c.title,
          c.summary,
          c.readme_bucket,
          c.readme_key,
          c.published_at,
          count(d.id) filter (where d.visibility = 'listed') as dataset_count
        from collections c
        left join datasets d on d.collection_id = c.id
        group by c.id
        order by c.published_at desc nulls last, c.title asc
        """
    ).fetchall()


def load_admin_collection_page(
    connection,
    *,
    search: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict], int]:
    search_term = search.strip() if search else ""
    params: dict[str, object] = {
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }
    where_clause = ""
    if search_term:
        params["search"] = f"%{search_term}%"
        where_clause = """
        where (
          c.title ilike %(search)s
          or c.slug ilike %(search)s
          or coalesce(c.summary, '') ilike %(search)s
        )
        """

    total = connection.execute(
        f"""
        select count(*)
        from collections c
        {where_clause}
        """,
        params,
    ).fetchone()["count"]
    total_pages = max(1, ceil(total / page_size)) if total else 1
    effective_page = min(max(page, 1), total_pages)
    params["offset"] = (effective_page - 1) * page_size
    rows = connection.execute(
        f"""
        select
          c.id,
          c.slug,
          c.title,
          c.summary,
          c.readme_bucket,
          c.readme_key,
          c.published_at,
          count(d.id) filter (where d.visibility = 'listed') as dataset_count
        from collections c
        left join datasets d on d.collection_id = c.id
        {where_clause}
        group by c.id
        order by c.published_at desc nulls last, c.title asc
        limit %(limit)s offset %(offset)s
        """,
        params,
    ).fetchall()
    return rows, total


def load_admin_collection_row(connection, collection_id: UUID) -> dict | None:
    return connection.execute(
        """
        select
          id,
          slug,
          title,
          summary,
          readme_bucket,
          readme_key,
          published_at
        from collections
        where id = %(collection_id)s
        """,
        {"collection_id": str(collection_id)},
    ).fetchone()


def load_admin_dataset_rows(connection, collection_id: UUID | None = None) -> list[dict]:
    query = """
        select
          d.id,
          d.collection_id,
          c.title as collection_title,
          d.slug,
          d.title,
          d.summary,
          d.classification,
          d.visibility,
          d.storage_bucket,
          d.storage_key,
          d.file_size_bytes,
          d.sort_order,
          d.published_at,
          coalesce(string_agg(distinct t.tag, ', ' order by t.tag), '') as tags
        from datasets d
        left join collections c on c.id = d.collection_id
        left join dataset_tags t on t.dataset_id = d.id
    """
    params: dict[str, str] = {}
    if collection_id:
        query += " where d.collection_id = %(collection_id)s"
        params["collection_id"] = str(collection_id)
    query += """
        group by d.id, c.title
        order by coalesce(c.title, ''), d.sort_order asc, d.published_at desc nulls last, d.title asc
    """
    return connection.execute(query, params).fetchall()


def load_admin_dataset_page(
    connection,
    *,
    collection_id: UUID | None = None,
    search: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict], int]:
    params: dict[str, object] = {
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }
    conditions: list[str] = []
    if collection_id:
        conditions.append("d.collection_id = %(collection_id)s")
        params["collection_id"] = str(collection_id)
    search_term = search.strip() if search else ""
    if search_term:
        params["search"] = f"%{search_term}%"
        conditions.append(
            """
            (
              d.title ilike %(search)s
              or d.slug ilike %(search)s
              or coalesce(d.summary, '') ilike %(search)s
              or d.storage_bucket ilike %(search)s
              or d.storage_key ilike %(search)s
              or coalesce(c.title, '') ilike %(search)s
              or coalesce(t.tag, '') ilike %(search)s
            )
            """
        )
    where_clause = f"where {' and '.join(conditions)}" if conditions else ""

    total = connection.execute(
        f"""
        select count(distinct d.id)
        from datasets d
        left join collections c on c.id = d.collection_id
        left join dataset_tags t on t.dataset_id = d.id
        {where_clause}
        """,
        params,
    ).fetchone()["count"]
    total_pages = max(1, ceil(total / page_size)) if total else 1
    effective_page = min(max(page, 1), total_pages)
    params["offset"] = (effective_page - 1) * page_size

    rows = connection.execute(
        f"""
        select
          d.id,
          d.collection_id,
          c.title as collection_title,
          d.slug,
          d.title,
          d.summary,
          d.classification,
          d.visibility,
          d.storage_bucket,
          d.storage_key,
          d.file_size_bytes,
          d.sort_order,
          d.published_at,
          coalesce(string_agg(distinct t.tag, ', ' order by t.tag), '') as tags
        from datasets d
        left join collections c on c.id = d.collection_id
        left join dataset_tags t on t.dataset_id = d.id
        {where_clause}
        group by d.id, c.title
        order by coalesce(c.title, ''), d.sort_order asc, d.published_at desc nulls last, d.title asc
        limit %(limit)s offset %(offset)s
        """,
        params,
    ).fetchall()
    return rows, total


def load_admin_dataset_row(connection, dataset_id: UUID) -> dict | None:
    return connection.execute(
        """
        select
          d.id,
          d.collection_id,
          d.slug,
          d.title,
          d.summary,
          d.classification,
          d.visibility,
          d.storage_bucket,
          d.storage_key,
          d.mime_type,
          d.file_size_bytes,
          d.sort_order,
          d.published_at,
          coalesce(string_agg(distinct t.tag, ', ' order by t.tag), '') as tags
        from datasets d
        left join dataset_tags t on t.dataset_id = d.id
        where d.id = %(dataset_id)s
        group by d.id
        """,
        {"dataset_id": str(dataset_id)},
    ).fetchone()


def load_dataset_choices(connection) -> list[dict]:
    return connection.execute(
        """
        select d.id, d.title, d.slug, c.title as collection_title
        from datasets d
        left join collections c on c.id = d.collection_id
        order by coalesce(c.title, ''), d.title asc
        """
    ).fetchall()


def load_collection_choices(connection) -> list[dict]:
    return connection.execute(
        """
        select id, title, slug
        from collections
        order by title asc
        """
    ).fetchall()


def load_admin_token_rows(connection) -> list[dict]:
    return connection.execute(
        """
        select
          t.id,
          t.label,
          t.status,
          t.expires_at,
          t.created_at,
          coalesce(
            json_agg(
              json_build_object(
                'id', g.id,
                'dataset_id', g.dataset_id,
                'classification', g.classification,
                'bucket', g.bucket,
                'key_prefix', g.key_prefix,
                'effect', g.effect
              )
              order by g.created_at asc
            ) filter (where g.id is not null),
            '[]'::json
          ) as grants
        from access_tokens t
        left join token_grants g on g.token_id = t.id
        group by t.id
        order by t.created_at desc
        """
    ).fetchall()


def load_bucket_names(s3_client: BaseClient) -> list[str]:
    try:
        buckets = s3_client.list_buckets().get("Buckets", [])
    except ClientError:
        return []
    return sorted(bucket["Name"] for bucket in buckets)


def load_bucket_objects(
    s3_client: BaseClient, *, bucket_name: str, prefix: str | None = None
) -> list[dict[str, str | int]]:
    normalized_prefix = (prefix or "").lstrip("/")
    paginator = s3_client.get_paginator("list_objects_v2")
    objects: list[dict[str, str | int]] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=normalized_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            objects.append(
                {
                    "key": key,
                    "file_size_bytes": int(obj.get("Size", 0)),
                }
            )
    return objects


def load_readme_options(s3_client: BaseClient) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    for bucket_name in load_bucket_names(s3_client):
        paginator = s3_client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.lower().endswith(".pdf"):
                        continue
                    options.append(
                        {
                            "bucket": bucket_name,
                            "key": key,
                            "label": f"{bucket_name} / {key}",
                        }
                    )
        except ClientError:
            continue
    return options


def token_grants_dataset(dataset: DatasetRecord, grants: list[TokenGrant]) -> bool:
    for grant in grants:
        if grant.dataset_id and grant.dataset_id != dataset.id:
            continue

        if grant.classification and grant.classification != dataset.classification:
            continue

        if grant.bucket and grant.bucket != dataset.storage_bucket:
            continue

        prefix = (grant.key_prefix or "").lstrip("/")
        if prefix and not (
            dataset.storage_key == prefix
            or dataset.storage_key.startswith(prefix.rstrip("/") + "/")
        ):
            continue

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


def build_catalog_item(dataset: DatasetRecord, grants: list[TokenGrant]) -> CatalogItem:
    downloadable, access_reason = evaluate_dataset_access(dataset, grants)
    return CatalogItem(
        id=dataset.id,
        collection_id=dataset.collection_id,
        slug=dataset.slug,
        filename=object_public_filename(dataset.storage_key),
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


def build_collection_counts(
    datasets: list[DatasetRecord], grants: list[TokenGrant]
) -> CollectionCounts:
    downloadable = 0
    for dataset in datasets:
        if evaluate_dataset_access(dataset, grants)[0]:
            downloadable += 1

    total = len(datasets)
    return CollectionCounts(
        total=total,
        public=sum(dataset.classification == "public" for dataset in datasets),
        restricted=sum(dataset.classification == "restricted" for dataset in datasets),
        confidential=sum(
            dataset.classification == "confidential" for dataset in datasets
        ),
        downloadable=downloadable,
        locked=total - downloadable,
    )


def build_collection_summary(
    collection: CollectionRecord,
    datasets: list[DatasetRecord],
    grants: list[TokenGrant],
) -> CollectionSummary:
    search_terms = [
        collection.title,
        collection.slug,
        collection.summary or "",
    ]
    for dataset in datasets:
        search_terms.extend(
            [
                dataset.title,
                dataset.slug,
                dataset.summary or "",
                *dataset.tags,
            ]
        )

    return CollectionSummary(
        id=collection.id,
        slug=collection.slug,
        title=collection.title,
        summary=collection.summary,
        published_at=collection.published_at,
        counts=build_collection_counts(datasets, grants),
        search_text=" ".join(term for term in search_terms if term).strip(),
    )


def object_public_filename(key: str) -> str:
    return key.rsplit("/", 1)[-1] if key else "download"


def generate_object_download_url(
    *,
    bucket: str,
    key: str,
    s3_client: BaseClient,
    presign_client: BaseClient,
    settings: Settings,
    download_filename: str | None = None,
) -> tuple[str, int | None]:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
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

    params = {"Bucket": bucket, "Key": key}
    if download_filename:
        params["ResponseContentDisposition"] = (
            f'attachment; filename="{download_filename}"'
        )

    try:
        url = presign_client.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=settings.presigned_url_ttl_seconds,
        )
    except ClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to create presigned URL.",
        ) from exc

    return url, settings.presigned_url_ttl_seconds


def generate_dataset_download_url(
    dataset: DatasetRecord,
    *,
    s3_client: BaseClient,
    presign_client: BaseClient,
    settings: Settings,
    download_filename: str | None,
) -> tuple[str, int | None]:
    if dataset.direct_public_url:
        return dataset.direct_public_url, None
    return generate_object_download_url(
        bucket=dataset.storage_bucket,
        key=dataset.storage_key,
        s3_client=s3_client,
        presign_client=presign_client,
        settings=settings,
        download_filename=download_filename,
    )


def build_collection_detail(
    collection: CollectionRecord,
    datasets: list[DatasetRecord],
    grants: list[TokenGrant],
    *,
    s3_client: BaseClient,
    presign_client: BaseClient,
    settings: Settings,
) -> CollectionDetail:
    readme_url = None
    readme_filename = None
    if collection.readme_bucket and collection.readme_key:
        try:
            readme_url, _ = generate_object_download_url(
                bucket=collection.readme_bucket,
                key=collection.readme_key,
                s3_client=s3_client,
                presign_client=presign_client,
                settings=settings,
                download_filename=None,
            )
            readme_filename = object_public_filename(collection.readme_key)
        except HTTPException as exc:
            if exc.status_code != status.HTTP_404_NOT_FOUND:
                raise

    return CollectionDetail(
        id=collection.id,
        slug=collection.slug,
        title=collection.title,
        summary=collection.summary,
        published_at=collection.published_at,
        readme_url=readme_url,
        readme_filename=readme_filename,
        counts=build_collection_counts(datasets, grants),
        datasets=[build_catalog_item(dataset, grants) for dataset in datasets],
    )


def build_unique_slug(base_slug: str, existing_slugs: set[str]) -> str:
    candidate = base_slug
    counter = 2
    while candidate in existing_slugs:
        candidate = f"{base_slug}-{counter}"
        counter += 1
    existing_slugs.add(candidate)
    return candidate


def insert_dataset_tags(connection, dataset_id: str, tags: list[str]) -> None:
    for tag in tags:
        connection.execute(
            """
            insert into dataset_tags (dataset_id, tag)
            values (%(dataset_id)s, %(tag)s)
            on conflict do nothing
            """,
            {"dataset_id": dataset_id, "tag": tag},
        )


def build_pager(
    *,
    page: int,
    total_items: int,
    page_size: int,
) -> dict[str, int | bool]:
    total_pages = max(1, ceil(total_items / page_size)) if total_items else 1
    current_page = min(max(page, 1), total_pages)
    start_item = 0 if total_items == 0 else (current_page - 1) * page_size + 1
    end_item = min(current_page * page_size, total_items) if total_items else 0
    return {
        "page": current_page,
        "page_size": page_size,
        "total_items": total_items,
        "total_pages": total_pages,
        "start_item": start_item,
        "end_item": end_item,
        "has_prev": current_page > 1,
        "has_next": current_page < total_pages,
        "prev_page": current_page - 1,
        "next_page": current_page + 1,
    }


def admin_context(
    request: Request,
    connection,
    s3_client: BaseClient,
    *,
    message: str | None = None,
    error: str | None = None,
    plaintext_token: str | None = None,
    edit_collection: dict | None = None,
    edit_dataset: dict | None = None,
    current_section: str = "catalog",
    active_tab: str = "list-data",
    selected_collection_id: UUID | None = None,
    collection_search: str = "",
    dataset_search: str = "",
    collection_page: int = 1,
    dataset_page: int = 1,
) -> dict:
    page_size = 20
    collections, collection_total = load_admin_collection_page(
        connection,
        search=collection_search,
        page=collection_page,
        page_size=page_size,
    )
    datasets, dataset_total = load_admin_dataset_page(
        connection,
        collection_id=selected_collection_id,
        search=dataset_search,
        page=dataset_page,
        page_size=page_size,
    )
    return {
        "request": request,
        "message": message,
        "error": error,
        "plaintext_token": plaintext_token,
        "current_section": current_section,
        "active_tab": active_tab,
        "tokens": load_admin_token_rows(connection),
        "collections": collections,
        "datasets": datasets,
        "dataset_choices": load_dataset_choices(connection),
        "collection_choices": load_collection_choices(connection),
        "bucket_names": load_bucket_names(s3_client),
        "readme_options": load_readme_options(s3_client),
        "edit_collection": edit_collection,
        "edit_dataset": edit_dataset,
        "selected_collection_id": str(selected_collection_id) if selected_collection_id else "",
        "collection_search": collection_search,
        "dataset_search": dataset_search,
        "collection_pager": build_pager(
            page=collection_page,
            total_items=collection_total,
            page_size=page_size,
        ),
        "dataset_pager": build_pager(
            page=dataset_page,
            total_items=dataset_total,
            page_size=page_size,
        ),
    }


app = FastAPI(title="Real Good Research Broker API", version="0.3.0")
app.mount("/admin/static", StaticFiles(directory=str(APP_DIR / "static")), name="admin-static")


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
    return CatalogResponse(items=[build_catalog_item(dataset, grants) for dataset in datasets])


@app.get("/v1/collections", response_model=CollectionsResponse)
def collections(
    auth_context: AuthContext | None = Depends(load_auth_context),
    connection=Depends(get_db_connection),
) -> CollectionsResponse:
    grants = load_token_grants(connection, auth_context.token_id if auth_context else None)
    items: list[CollectionSummary] = []
    for collection in load_collections(connection):
        datasets = load_collection_datasets(connection, collection.id)
        items.append(build_collection_summary(collection, datasets, grants))
    return CollectionsResponse(items=items)


@app.get("/v1/collections/{slug}", response_model=CollectionDetail)
def collection_detail(
    slug: str,
    auth_context: AuthContext | None = Depends(load_auth_context),
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    presign_client: BaseClient = Depends(get_presign_s3_client),
    settings: Settings = Depends(get_settings),
) -> CollectionDetail:
    collection = load_collection_by_slug(connection, slug)
    if not collection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Collection not found.",
        )

    grants = load_token_grants(connection, auth_context.token_id if auth_context else None)
    datasets = load_collection_datasets(connection, collection.id)
    return build_collection_detail(
        collection,
        datasets,
        grants,
        s3_client=s3_client,
        presign_client=presign_client,
        settings=settings,
    )


@app.get("/v1/datasets/{slug}", response_model=CatalogItem)
def dataset_detail(
    slug: str,
    auth_context: AuthContext | None = Depends(load_auth_context),
    connection=Depends(get_db_connection),
) -> CatalogItem:
    dataset = load_dataset_by_slug(connection, slug)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found.",
        )

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

    download_url, expires_in = generate_dataset_download_url(
        dataset,
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


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request, settings: Settings = Depends(get_settings)):
    if admin_authenticated(request, settings):
        return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(
        request,
        "admin_login.html",
        {"error": None},
    )


@app.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    form = await request.form()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))

    if (
        username != settings.admin_username
        or not hmac.compare_digest(password, settings.admin_password)
    ):
        return templates.TemplateResponse(
            request,
            "admin_login.html",
            {"error": "Invalid admin credentials."},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    response = RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
    set_admin_session_cookie(response, settings)
    return response


@app.post("/admin/logout")
def admin_logout():
    response = RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)
    clear_admin_session_cookie(response)
    return response


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()
    return RedirectResponse(url="/admin/catalog", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/admin/catalog", response_class=HTMLResponse)
def admin_catalog(
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    selected_collection_id = parse_optional_uuid(
        request.query_params.get("collection_id")
    )
    active_tab = str(request.query_params.get("tab", "list-data")).strip() or "list-data"
    collection_search = str(request.query_params.get("collection_q", "")).strip()
    dataset_search = str(request.query_params.get("dataset_q", "")).strip()
    collection_page = parse_page(request.query_params.get("collection_page"))
    dataset_page = parse_page(request.query_params.get("dataset_page"))
    return templates.TemplateResponse(
        request,
        "admin_catalog.html",
        admin_context(
            request,
            connection,
            s3_client,
            current_section="catalog",
            active_tab=active_tab,
            selected_collection_id=selected_collection_id,
            collection_search=collection_search,
            dataset_search=dataset_search,
            collection_page=collection_page,
            dataset_page=dataset_page,
        ),
    )


@app.get("/admin/tokens", response_class=HTMLResponse)
def admin_tokens_page(
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    return templates.TemplateResponse(
        request,
        "admin_tokens.html",
        admin_context(
            request,
            connection,
            s3_client,
            current_section="tokens",
            active_tab="tokens",
        ),
    )


@app.post("/admin/tokens", response_class=HTMLResponse)
async def admin_create_token(
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    form = await request.form()
    label = str(form.get("label", "")).strip()
    plaintext_token = str(form.get("plaintext_token", "")).strip()
    expires_at = parse_optional_datetime(str(form.get("expires_at", "")).strip() or None)
    grant_mode = str(form.get("grant_mode", "bucket")).strip()
    dataset_id = parse_optional_uuid(str(form.get("dataset_id", "")).strip() or None)
    classification = str(form.get("classification", "restricted")).strip() or None
    bucket = str(form.get("bucket", "")).strip() or None
    key_prefix = str(form.get("key_prefix", "")).strip() or None

    if not label:
        return templates.TemplateResponse(
            request,
            "admin_tokens.html",
            admin_context(request, connection, s3_client, error="Token label is required.", current_section="tokens", active_tab="tokens"),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if grant_mode == "bucket" and not bucket:
        return templates.TemplateResponse(
            request,
            "admin_tokens.html",
            admin_context(request, connection, s3_client, error="Bucket grants require a bucket name.", current_section="tokens", active_tab="tokens"),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if grant_mode == "dataset" and not dataset_id:
        return templates.TemplateResponse(
            request,
            "admin_tokens.html",
            admin_context(request, connection, s3_client, error="Dataset grants require a dataset selection.", current_section="tokens", active_tab="tokens"),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    random_suffix = secrets.token_urlsafe(12)
    if plaintext_token:
        plaintext_token = f"{plaintext_token}_{random_suffix}"
    else:
        plaintext_token = random_suffix
    token_hash = fingerprint_token(plaintext_token)

    existing_token = connection.execute(
        """
        select 1
        from access_tokens
        where token_hash = %(token_hash)s
        """,
        {"token_hash": token_hash},
    ).fetchone()
    if existing_token:
        return templates.TemplateResponse(
            request,
            "admin_tokens.html",
            admin_context(
                request,
                connection,
                s3_client,
                error="That generated token already exists. Try again or use a different prefix.",
                current_section="tokens",
                active_tab="tokens",
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    try:
        token_row = connection.execute(
            """
            insert into access_tokens (token_hash, label, status, expires_at)
            values (%(token_hash)s, %(label)s, 'active', %(expires_at)s)
            returning id
            """,
            {
                "token_hash": token_hash,
                "label": label,
                "expires_at": expires_at,
            },
        ).fetchone()

        if grant_mode == "dataset" and dataset_id:
            connection.execute(
                """
                insert into token_grants (token_id, dataset_id, effect)
                values (%(token_id)s, %(dataset_id)s, 'allow')
                """,
                {"token_id": token_row["id"], "dataset_id": str(dataset_id)},
            )
        elif grant_mode == "bucket":
            connection.execute(
                """
                insert into token_grants (token_id, classification, bucket, key_prefix, effect)
                values (%(token_id)s, %(classification)s, %(bucket)s, %(key_prefix)s, 'allow')
                """,
                {
                    "token_id": token_row["id"],
                    "classification": classification,
                    "bucket": bucket,
                    "key_prefix": key_prefix,
                },
            )

        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return templates.TemplateResponse(
        request,
        "admin_tokens.html",
        admin_context(
            request,
            connection,
            s3_client,
            message="Token created. Copy the plaintext token now; it will not be shown again.",
            plaintext_token=plaintext_token,
            current_section="tokens",
            active_tab="tokens",
        ),
    )


@app.post("/admin/tokens/{token_id}/revoke")
def admin_revoke_token(
    token_id: UUID,
    request: Request,
    connection=Depends(get_db_connection),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    connection.execute(
        """
        update access_tokens
        set status = 'revoked'
        where id = %(token_id)s
        """,
        {"token_id": str(token_id)},
    )
    connection.commit()
    return RedirectResponse(url="/admin/tokens", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/collections", response_class=HTMLResponse)
async def admin_create_collection(
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    form = await request.form()
    title = str(form.get("title", "")).strip()
    slug = str(form.get("slug", "")).strip()
    summary = str(form.get("summary", "")).strip() or None
    readme_bucket, readme_key = parse_readme_selection(
        str(form.get("readme_object", "")).strip() or None
    )
    published_at = parse_optional_datetime(str(form.get("published_at", "")).strip() or None)

    if not title or not slug:
        return templates.TemplateResponse(
            request,
            "admin_catalog.html",
            admin_context(request, connection, s3_client, error="Collection title and slug are required.", current_section="catalog", active_tab="create-collection"),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    try:
        connection.execute(
            """
            insert into collections (title, slug, summary, readme_bucket, readme_key, published_at)
            values (%(title)s, %(slug)s, %(summary)s, %(readme_bucket)s, %(readme_key)s, coalesce(%(published_at)s, now()))
            """,
            {
                "title": title,
                "slug": slug,
                "summary": summary,
                "readme_bucket": readme_bucket,
                "readme_key": readme_key,
                "published_at": published_at,
            },
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return templates.TemplateResponse(
        request,
        "admin_catalog.html",
        admin_context(request, connection, s3_client, message="Collection created.", current_section="catalog", active_tab="create-collection"),
    )


@app.get("/admin/collections/{collection_id}", response_class=HTMLResponse)
def admin_edit_collection_page(
    collection_id: UUID,
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()
    edit_collection = load_admin_collection_row(connection, collection_id)
    if not edit_collection:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Collection not found.")
    return templates.TemplateResponse(
        request,
        "admin_collection_edit.html",
        admin_context(request, connection, s3_client, edit_collection=edit_collection, active_tab="create-collection"),
        
    )


@app.post("/admin/collections/{collection_id}", response_class=HTMLResponse)
async def admin_update_collection(
    collection_id: UUID,
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    form = await request.form()
    title = str(form.get("title", "")).strip()
    slug = str(form.get("slug", "")).strip()
    summary = str(form.get("summary", "")).strip() or None
    readme_bucket, readme_key = parse_readme_selection(
        str(form.get("readme_object", "")).strip() or None
    )
    published_at = parse_optional_datetime(str(form.get("published_at", "")).strip() or None)

    try:
        connection.execute(
            """
            update collections
            set
              title = %(title)s,
              slug = %(slug)s,
              summary = %(summary)s,
              readme_bucket = %(readme_bucket)s,
              readme_key = %(readme_key)s,
              published_at = coalesce(%(published_at)s, published_at)
            where id = %(collection_id)s
            """,
            {
                "collection_id": str(collection_id),
                "title": title,
                "slug": slug,
                "summary": summary,
                "readme_bucket": readme_bucket,
                "readme_key": readme_key,
                "published_at": published_at,
            },
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    edit_collection = load_admin_collection_row(connection, collection_id)
    return templates.TemplateResponse(
        request,
        "admin_collection_edit.html",
        admin_context(
            request,
            connection,
            s3_client,
            message="Collection updated.",
            edit_collection=edit_collection,
            current_section="catalog",
            active_tab="create-collection",
        ),
    )


@app.post("/admin/datasets", response_class=HTMLResponse)
async def admin_create_dataset(
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    form = await request.form()
    title = str(form.get("title", "")).strip()
    slug = str(form.get("slug", "")).strip()
    if not title or not slug:
        return templates.TemplateResponse(
            request,
            "admin_catalog.html",
            admin_context(request, connection, s3_client, error="Dataset title and slug are required.", current_section="catalog", active_tab="import-data"),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if not str(form.get("storage_bucket", "")).strip() or not str(form.get("storage_key", "")).strip():
        return templates.TemplateResponse(
            request,
            "admin_catalog.html",
            admin_context(request, connection, s3_client, error="Dataset bucket and object key are required.", current_section="catalog", active_tab="import-data"),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    tags = normalize_tags(str(form.get("tags", "")).strip() or None)
    collection_id = parse_optional_uuid(str(form.get("collection_id", "")).strip() or None)
    published_at = parse_optional_datetime(str(form.get("published_at", "")).strip() or None)
    file_size_bytes = parse_optional_int(str(form.get("file_size_bytes", "")).strip() or None)
    sort_order = parse_optional_int(str(form.get("sort_order", "")).strip() or None) or 0

    try:
        row = connection.execute(
            """
            insert into datasets (
              collection_id, slug, title, summary, classification, visibility,
              storage_bucket, storage_key, mime_type, file_size_bytes, sort_order, published_at
            )
            values (
              %(collection_id)s, %(slug)s, %(title)s, %(summary)s, %(classification)s, %(visibility)s,
              %(storage_bucket)s, %(storage_key)s, %(mime_type)s, %(file_size_bytes)s, %(sort_order)s,
              coalesce(%(published_at)s, now())
            )
            returning id
            """,
            {
                "collection_id": str(collection_id) if collection_id else None,
                "slug": slug,
                "title": title,
                "summary": str(form.get("summary", "")).strip() or None,
                "classification": str(form.get("classification", "public")).strip(),
                "visibility": str(form.get("visibility", "listed")).strip(),
                "storage_bucket": str(form.get("storage_bucket", "")).strip(),
                "storage_key": str(form.get("storage_key", "")).strip(),
                "mime_type": str(form.get("mime_type", "")).strip() or None,
                "file_size_bytes": file_size_bytes,
                "sort_order": sort_order,
                "published_at": published_at,
            },
        ).fetchone()

        insert_dataset_tags(connection, str(row["id"]), tags)

        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return templates.TemplateResponse(
        request,
        "admin_catalog.html",
        admin_context(request, connection, s3_client, message="Dataset created.", current_section="catalog", active_tab="import-data"),
    )


@app.post("/admin/datasets/import", response_class=HTMLResponse)
async def admin_import_datasets(
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    form = await request.form()
    collection_id = parse_optional_uuid(str(form.get("collection_id", "")).strip() or None)
    bucket = str(form.get("storage_bucket", "")).strip()
    prefix = str(form.get("storage_prefix", "")).strip()
    visibility = str(form.get("visibility", "listed")).strip() or "listed"
    selected_collection_id = collection_id

    if not collection_id:
        return templates.TemplateResponse(
            request,
            "admin_catalog.html",
            admin_context(
                request,
                connection,
                s3_client,
                error="Bulk import requires a target collection.",
                current_section="catalog",
                active_tab="import-data",
                selected_collection_id=selected_collection_id,
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if not bucket:
        return templates.TemplateResponse(
            request,
            "admin_catalog.html",
            admin_context(
                request,
                connection,
                s3_client,
                error="Bulk import requires a bucket name.",
                current_section="catalog",
                active_tab="import-data",
                selected_collection_id=selected_collection_id,
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    try:
        objects = load_bucket_objects(s3_client, bucket_name=bucket, prefix=prefix or None)
    except ClientError as exc:
        return templates.TemplateResponse(
            request,
            "admin_catalog.html",
            admin_context(
                request,
                connection,
                s3_client,
                error=f"Unable to read objects from {bucket}.",
                current_section="catalog",
                active_tab="import-data",
                selected_collection_id=selected_collection_id,
            ),
            status_code=status.HTTP_502_BAD_GATEWAY,
        )

    if not objects:
        return templates.TemplateResponse(
            request,
            "admin_catalog.html",
            admin_context(
                request,
                connection,
                s3_client,
                error="No objects matched that bucket/prefix.",
                current_section="catalog",
                active_tab="import-data",
                selected_collection_id=selected_collection_id,
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    existing_rows = connection.execute(
        """
        select storage_bucket, storage_key, slug
        from datasets
        """
    ).fetchall()
    existing_keys = {
        (row["storage_bucket"], row["storage_key"])
        for row in existing_rows
        if row["storage_bucket"] and row["storage_key"]
    }
    existing_slugs = {row["slug"] for row in existing_rows if row["slug"]}

    imported = 0
    skipped = 0

    try:
        for sort_order, obj in enumerate(objects, start=1):
            key = str(obj["key"])
            if (bucket, key) in existing_keys:
                skipped += 1
                continue

            title = object_title_from_key(key)
            slug = build_unique_slug(slugify(title), existing_slugs)
            connection.execute(
                """
                insert into datasets (
                  collection_id, slug, title, summary, classification, visibility,
                  storage_bucket, storage_key, file_size_bytes, sort_order, published_at
                )
                values (
                  %(collection_id)s, %(slug)s, %(title)s, null, 'confidential', %(visibility)s,
                  %(storage_bucket)s, %(storage_key)s, %(file_size_bytes)s, %(sort_order)s, now()
                )
                """,
                {
                    "collection_id": str(collection_id),
                    "slug": slug,
                    "title": title,
                    "visibility": visibility,
                    "storage_bucket": bucket,
                    "storage_key": key,
                    "file_size_bytes": int(obj["file_size_bytes"]),
                    "sort_order": sort_order,
                },
            )
            imported += 1
            existing_keys.add((bucket, key))

        connection.commit()
    except Exception:
        connection.rollback()
        raise

    message = f"Imported {imported} dataset"
    if imported != 1:
        message += "s"
    if skipped:
        message += f"; skipped {skipped} already-cataloged object"
        if skipped != 1:
            message += "s"
    message += "."

    return templates.TemplateResponse(
        request,
        "admin_catalog.html",
        admin_context(
            request,
            connection,
            s3_client,
            message=message,
            current_section="catalog",
            active_tab="import-data",
            selected_collection_id=selected_collection_id,
        ),
    )


@app.get("/admin/datasets/{dataset_id}", response_class=HTMLResponse)
def admin_edit_dataset_page(
    dataset_id: UUID,
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()
    edit_dataset = load_admin_dataset_row(connection, dataset_id)
    if not edit_dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found.")
    return templates.TemplateResponse(
        request,
        "admin_dataset_edit.html",
        admin_context(request, connection, s3_client, edit_dataset=edit_dataset, active_tab="list-data"),
    )


@app.post("/admin/datasets/{dataset_id}", response_class=HTMLResponse)
async def admin_update_dataset(
    dataset_id: UUID,
    request: Request,
    connection=Depends(get_db_connection),
    s3_client: BaseClient = Depends(get_s3_client),
    settings: Settings = Depends(get_settings),
):
    if not admin_authenticated(request, settings):
        return admin_login_redirect()

    form = await request.form()
    tags = normalize_tags(str(form.get("tags", "")).strip() or None)
    collection_id = parse_optional_uuid(str(form.get("collection_id", "")).strip() or None)
    published_at = parse_optional_datetime(str(form.get("published_at", "")).strip() or None)
    file_size_bytes = parse_optional_int(str(form.get("file_size_bytes", "")).strip() or None)
    sort_order = parse_optional_int(str(form.get("sort_order", "")).strip() or None) or 0

    try:
        connection.execute(
            """
            update datasets
            set
              collection_id = %(collection_id)s,
              slug = %(slug)s,
              title = %(title)s,
              summary = %(summary)s,
              classification = %(classification)s,
              visibility = %(visibility)s,
              storage_bucket = %(storage_bucket)s,
              storage_key = %(storage_key)s,
              mime_type = %(mime_type)s,
              file_size_bytes = %(file_size_bytes)s,
              sort_order = %(sort_order)s,
              published_at = coalesce(%(published_at)s, published_at)
            where id = %(dataset_id)s
            """,
            {
                "dataset_id": str(dataset_id),
                "collection_id": str(collection_id) if collection_id else None,
                "slug": str(form.get("slug", "")).strip(),
                "title": str(form.get("title", "")).strip(),
                "summary": str(form.get("summary", "")).strip() or None,
                "classification": str(form.get("classification", "public")).strip(),
                "visibility": str(form.get("visibility", "listed")).strip(),
                "storage_bucket": str(form.get("storage_bucket", "")).strip(),
                "storage_key": str(form.get("storage_key", "")).strip(),
                "mime_type": str(form.get("mime_type", "")).strip() or None,
                "file_size_bytes": file_size_bytes,
                "sort_order": sort_order,
                "published_at": published_at,
            },
        )
        connection.execute(
            "delete from dataset_tags where dataset_id = %(dataset_id)s",
            {"dataset_id": str(dataset_id)},
        )
        for tag in tags:
            connection.execute(
                """
                insert into dataset_tags (dataset_id, tag)
                values (%(dataset_id)s, %(tag)s)
                on conflict do nothing
                """,
                {"dataset_id": str(dataset_id), "tag": tag},
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    edit_dataset = load_admin_dataset_row(connection, dataset_id)
    return templates.TemplateResponse(
        request,
        "admin_dataset_edit.html",
        admin_context(
            request,
            connection,
            s3_client,
            message="Dataset updated.",
            edit_dataset=edit_dataset,
            current_section="catalog",
            active_tab="list-data",
        ),
    )
