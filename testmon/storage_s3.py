import os
import random
import tempfile
import time

from testmon import db as testmon_db
from testmon.common import get_logger

try:
    import boto3
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

logger = get_logger(__name__)

_MAX_RETRIES = 10
_RETRY_BASE_SLEEP = 0.5


def _parse_s3_url(url):
    if not url.startswith("s3://"):
        raise ValueError(f"S3 URL must start with s3://, got: {url!r}")
    rest = url[5:]
    bucket, _, key = rest.partition("/")
    if not bucket:
        raise ValueError(f"S3 URL has no bucket: {url!r}")
    return bucket, key


class S3Storage:
    """
    Wraps a remote SQLite file stored in S3.

    Session lifecycle:
      setup()              – download the S3 file to a temp path, return a DB instance
      seed_from_fallback() – if the current branch has no data, copy from fallback_branch
      merge_and_upload()   – re-download latest, apply delta, upload with ETag CAS
      cleanup()            – delete temp file
    """

    def __init__(
        self, s3_url: str, readonly: bool = True, fallback_branch: str = "main"
    ):
        if not HAS_BOTO3:
            raise ImportError(
                "boto3 is required for --testmon-s3. Install it with: pip install boto3"
            )
        self.s3_url = s3_url
        self.readonly = readonly
        self.fallback_branch = fallback_branch
        self._bucket, self._key = _parse_s3_url(s3_url)
        self._s3 = boto3.client("s3")
        self._local_db_path: str | None = None
        self.local_db: testmon_db.DB | None = None
        self._current_etag: str | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def setup(self, force_pull: bool = False) -> testmon_db.DB:  # pylint: disable=unused-argument
        """
        Download the S3 object to a temp file and return an open DB.

        force_pull is accepted for forward-compatibility (future: skip download
        when local .testmondata already has fresh branch data) but currently
        always downloads — CI runners have no persistent local state anyway.
        """
        fd, self._local_db_path = tempfile.mkstemp(suffix=".testmondata.s3")
        os.close(fd)

        self._current_etag = self._download_to(self._local_db_path)
        if self._current_etag is None:
            logger.info(
                "testmon: no S3 cache found at %s — starting fresh", self.s3_url
            )
        else:
            logger.info("testmon: downloaded S3 cache from %s", self.s3_url)

        self.local_db = testmon_db.DB(self._local_db_path, readonly=False)
        return self.local_db

    def seed_from_fallback(
        self,
        environment_name: str,
        system_packages: str,
        python_version: str,
        branch: str,
    ) -> bool:
        """
        If the current branch has no data, copy rows from fallback_branch.
        Must be called after setup() but before TestmonData.for_local_run()
        so the seeded environment row is found by fetch_or_create_environment.
        """
        if not self.local_db or branch == self.fallback_branch or not branch:
            return False
        seeded = self.local_db.seed_from_branch(
            environment_name,
            system_packages,
            python_version,
            self.fallback_branch,
            branch,
        )
        if seeded:
            logger.info(
                "testmon: seeded branch %r from %r in local S3 cache",
                branch,
                self.fallback_branch,
            )
        return seeded

    def merge_and_upload(
        self,
        delta: dict,
        environment_name: str,
        system_packages: str,
        python_version: str,
        branch: str,
    ) -> None:
        """
        Re-download the latest S3 file, apply our delta, upload with ETag CAS.
        Retries on concurrent-write conflicts up to _MAX_RETRIES times.
        """
        if not delta:
            return

        for attempt in range(_MAX_RETRIES):
            fd, fresh_path = tempfile.mkstemp(suffix=".testmondata.merge")
            os.close(fd)
            try:
                etag = self._download_to(fresh_path)
                fresh_db = testmon_db.DB(fresh_path, readonly=False)

                exec_id, _ = fresh_db.fetch_or_create_environment(
                    environment_name, system_packages, python_version, branch
                )
                fresh_db.insert_test_file_fps(delta, exec_id)
                with fresh_db.con as con:
                    fresh_db.vacuum_file_fp(con)
                fresh_db.con.close()

                with open(fresh_path, "rb") as f:
                    data = f.read()

                put_kwargs: dict = {
                    "Bucket": self._bucket,
                    "Key": self._key,
                    "Body": data,
                }
                if etag is not None:
                    put_kwargs["IfMatch"] = etag
                else:
                    put_kwargs["IfNoneMatch"] = "*"

                self._s3.put_object(**put_kwargs)
                logger.info("testmon: S3 merge uploaded on attempt %d", attempt + 1)
                return

            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if (
                    code in ("PreconditionFailed", "ConditionalRequestConflicted")
                    and attempt < _MAX_RETRIES - 1
                ):
                    sleep = _RETRY_BASE_SLEEP * (2**attempt) * random.uniform(0.5, 1.5)
                    logger.info(
                        "testmon: S3 CAS conflict on attempt %d, retrying in %.1fs",
                        attempt + 1,
                        sleep,
                    )
                    time.sleep(sleep)
                    continue
                raise
            finally:
                try:
                    os.unlink(fresh_path)
                except FileNotFoundError:
                    pass

        raise RuntimeError(  # pragma: no cover
            f"testmon: failed to merge S3 cache after {_MAX_RETRIES} attempts"
        )

    def cleanup(self) -> None:
        if self.local_db is not None:
            try:
                self.local_db.con.close()
            except Exception:  # pylint: disable=broad-except
                pass
            self.local_db = None
        if self._local_db_path and os.path.exists(self._local_db_path):
            try:
                os.unlink(self._local_db_path)
            except FileNotFoundError:
                pass
            self._local_db_path = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _download_to(self, path: str) -> str | None:
        """Download the S3 object to *path*. Returns ETag or None if missing."""
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=self._key)
            with open(path, "wb") as f:
                f.write(response["Body"].read())
            return response["ETag"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
