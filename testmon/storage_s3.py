import os
import random
import sqlite3
import tempfile
import time

from testmon import db as testmon_db
from testmon.common import get_logger
from testmon.testmon_core import TestmonException

try:
    import boto3
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

logger = get_logger(__name__)

_MAX_RETRIES = 10
_RETRY_BASE_SLEEP = 0.5
_MAX_RETRY_SLEEP = 30.0


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
      setup()              – use local .testmondata if it exists, else download from S3
      seed_from_fallback() – if the current branch has no data, copy from fallback_branch
      merge_and_upload()   – re-download latest, apply delta, upload with ETag CAS
      cleanup()            – close the DB connection

    With read_branch set, setup() opens a private copy of that branch's object
    and the session neither seeds nor uploads.
    """

    def __init__(
        self,
        s3_url: str,
        readonly: bool = True,
        fallback_branch: str = "main",
        env_max_age_days: int = 30,
        max_envs_per_branch: int = 2,
        overwrite_branches: set[str] | None = None,
        read_branch: str | None = None,
    ):
        if not HAS_BOTO3:
            raise ImportError(
                "boto3 is required for --testmon-s3. Install it with: pip install boto3"
            )
        self.s3_url = s3_url
        # A read-branch snapshot is never written back.
        self.read_branch = read_branch or None
        self.readonly = readonly or self.read_branch is not None
        self.fallback_branch = fallback_branch
        self.env_max_age_days = env_max_age_days
        self.max_envs_per_branch = max_envs_per_branch
        self._overwrite_branches_config = overwrite_branches
        self._bucket, self._key = _parse_s3_url(s3_url)
        self._s3 = boto3.client("s3")
        self._local_db_path: str | None = None
        self.local_db: testmon_db.DB | None = None
        self._current_etag: str | None = None
        self._owns_local_db_file = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def setup(
        self,
        local_db_path: str,
        env_name: str,
        system_packages: str,
        python_version: str,
        branch: str,
        force_remote: bool = False,
        target_branch: str | None = None,
    ) -> testmon_db.DB:
        """
        Prepare the local DB for this session.

        With read_branch set (--testmon-s3-read-branch) none of the below applies:
        see _setup_read_branch().

        Decision tree:
        - Local file missing → merge from S3 (silent if S3 also empty).
        - Local file exists, current env found, no force_remote → use local as-is (fast).
        - Local file exists, current env not found → silently merge from S3.
        - force_remote → always fetch from S3; current env's local test data is replaced,
          all other local environments are preserved.

        S3 fallback chain for downloads:
          branch key → target_branch key (if given) → fallback_branch key
        """
        if self.read_branch:
            return self._setup_read_branch()

        self._local_db_path = local_db_path

        need_remote = force_remote or not os.path.exists(local_db_path)

        if not need_remote:
            probe = testmon_db.DB(local_db_path, readonly=False)
            env_found = (
                probe.con.execute(
                    "SELECT 1 FROM environment "
                    "WHERE environment_name=? AND system_packages=? "
                    "AND python_version=? AND branch=?",
                    (env_name, system_packages, python_version, branch),
                ).fetchone()
                is not None
            )
            probe.con.close()
            if env_found:
                logger.debug("testmon: using local cache at %s", local_db_path)
                self.local_db = testmon_db.DB(local_db_path, readonly=False)
                return self.local_db
            logger.debug("testmon: environment not in local cache, merging from remote")
            need_remote = True

        downloaded = []
        found_any = False
        try:
            for source_branch, key in self._download_chain(branch, target_branch):
                fd, tmp_path = tempfile.mkstemp(suffix=".testmondata.s3pull")
                os.close(fd)
                downloaded.append(tmp_path)
                if self._download_to(tmp_path, key=key) is None:
                    logger.debug("testmon: no S3 cache for branch %r", source_branch)
                    continue
                logger.info("testmon: downloaded S3 cache for branch %r", source_branch)
                merge_db = testmon_db.DB(local_db_path, readonly=False)
                if force_remote and not found_any:
                    self._clear_env(
                        merge_db, env_name, system_packages, python_version, branch
                    )
                found_any = True
                merge_db.merge_from_s3(
                    tmp_path, overwrite_branches=self._overwrite_branches()
                )
                merge_db.con.close()
                # Stop at the first object that holds the current environment
                # (same packages hash) for the branch it belongs to. Otherwise
                # keep going down the chain so seed_from_fallback() has a row for
                # the current packages hash to copy, instead of every test
                # re-running after a requirements change on the target branch.
                if self._has_environment(
                    tmp_path, env_name, system_packages, python_version, source_branch
                ):
                    break
                logger.debug(
                    "testmon: S3 cache for branch %r has no environment for the "
                    "current packages, trying the next branch",
                    source_branch,
                )
            if not found_any:
                logger.info(
                    "testmon: no S3 cache found at %s — starting fresh", self.s3_url
                )
        finally:
            for path in downloaded:
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass

        self.local_db = testmon_db.DB(local_db_path, readonly=False)
        return self.local_db

    @staticmethod
    def _clear_env(
        db: "testmon_db.DB",
        env_name: str,
        system_packages: str,
        python_version: str,
        branch: str,
    ) -> None:
        """Delete test_execution rows for the given environment so S3 data replaces them."""
        with db.con as con:
            con.execute(
                "DELETE FROM test_execution WHERE environment_id = ("
                "  SELECT id FROM environment "
                "  WHERE environment_name=? AND system_packages=? "
                "  AND python_version=? AND branch=?"
                ")",
                (env_name, system_packages, python_version, branch),
            )

    def seed_from_fallback(
        self,
        environment_name: str,
        system_packages: str,
        python_version: str,
        branch: str,
        target_branch: str | None = None,
    ) -> bool:
        """
        If the current branch has no data, copy rows into it.
        Tries target_branch first (for chained PRs), then fallback_branch.
        Must be called after setup() but before TestmonData.for_local_run()
        so the seeded environment row is found by fetch_or_create_environment.
        """
        if (
            not self.local_db
            or self.read_branch
            or branch == self.fallback_branch
            or not branch
        ):
            return False

        candidates = []
        if target_branch and target_branch != branch and target_branch != self.fallback_branch:
            candidates.append(target_branch)
        candidates.append(self.fallback_branch)

        for source in candidates:
            seeded = self.local_db.seed_from_branch(
                environment_name,
                system_packages,
                python_version,
                source,
                branch,
            )
            if seeded:
                logger.info(
                    "testmon: seeded branch %r from %r in local S3 cache",
                    branch,
                    source,
                )
                return True
        return False

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
        if not delta or self.readonly:
            return

        branch_key = self._branch_key(branch)

        for attempt in range(_MAX_RETRIES):
            fd, fresh_path = tempfile.mkstemp(suffix=".testmondata.merge")
            os.close(fd)
            try:
                etag = self._download_to(fresh_path, key=branch_key)
                fresh_db = testmon_db.DB(fresh_path, readonly=False)

                exec_id, _ = fresh_db.fetch_or_create_environment(
                    environment_name, system_packages, python_version, branch
                )
                fresh_db.insert_test_file_fps(delta, exec_id)
                with fresh_db.con as con:
                    fresh_db._cleanup_old_environments(
                        con,
                        days=self.env_max_age_days,
                        max_envs_per_branch=self.max_envs_per_branch,
                    )
                    fresh_db.vacuum_file_fp(con)
                # VACUUM must run outside any transaction and rebuilds the file
                # in-place, physically reclaiming pages freed by the deletes above.
                fresh_db.con.execute("VACUUM")
                fresh_db.con.close()

                with open(fresh_path, "rb") as f:
                    data = f.read()

                put_kwargs: dict = {
                    "Bucket": self._bucket,
                    "Key": branch_key,
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
                if code not in ("PreconditionFailed", "ConditionalRequestConflict"):
                    raise
                if attempt < _MAX_RETRIES - 1:
                    sleep = min(
                        _RETRY_BASE_SLEEP * (2**attempt) * random.uniform(0.5, 1.5),
                        _MAX_RETRY_SLEEP,
                    )
                    logger.info(
                        "testmon: S3 CAS conflict on attempt %d, retrying in %.1fs",
                        attempt + 1,
                        sleep,
                    )
                    time.sleep(sleep)
                    continue
                logger.warning(
                    "testmon: S3 merge failed after %d attempts due to concurrent writes"
                    " — results not saved to S3 this run",
                    _MAX_RETRIES,
                )
                return
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
        if self._owns_local_db_file and self._local_db_path:
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.unlink(self._local_db_path + suffix)
                except FileNotFoundError:
                    pass
            self._owns_local_db_file = False
        self._local_db_path = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _setup_read_branch(self) -> testmon_db.DB:
        """
        Open a private copy of the read branch's S3 object as this session's DB.

        The local .testmondata is neither read nor written, nothing is seeded,
        and the copy is deleted in cleanup(). The caller uses read_branch as the
        session's branch, so selection runs against that branch's environment.
        """
        fd, snapshot_path = tempfile.mkstemp(suffix=".testmondata.readbranch")
        os.close(fd)
        key = self._branch_key(self.read_branch)
        if self._download_to(snapshot_path, key=key) is None:
            os.unlink(snapshot_path)
            raise TestmonException(
                f"testmon: --testmon-s3-read-branch={self.read_branch!r} has no S3 "
                f"object at s3://{self._bucket}/{key}"
            )
        logger.info(
            "testmon: using read-only S3 snapshot of branch %r", self.read_branch
        )
        self._local_db_path = snapshot_path
        self._owns_local_db_file = True
        self.local_db = testmon_db.DB(snapshot_path, readonly=False)
        return self.local_db

    def _download_chain(self, branch: str, target_branch: str | None):
        """Yield (branch, key) pairs in download order, skipping duplicate keys."""
        seen = set()
        for candidate in (branch, target_branch, self.fallback_branch):
            if candidate is None:
                continue
            key = self._branch_key(candidate)
            if key in seen:
                continue
            seen.add(key)
            yield candidate, key

    def _overwrite_branches(self) -> set[str]:
        if self._overwrite_branches_config is not None:
            branches = set(self._overwrite_branches_config) | {self.fallback_branch}
        else:
            branches = {"main", "master", self.fallback_branch}
        return branches - {""}

    @staticmethod
    def _has_environment(
        path: str,
        env_name: str,
        system_packages: str,
        python_version: str,
        branch: str,
    ) -> bool:
        """True when the SQLite file at *path* has a row for this exact environment."""
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            return (
                con.execute(
                    "SELECT 1 FROM environment "
                    "WHERE environment_name=? AND system_packages=? "
                    "AND python_version=? AND branch=?",
                    (env_name, system_packages, python_version, branch),
                ).fetchone()
                is not None
            )
        except sqlite3.Error:
            return False
        finally:
            con.close()

    def _branch_key(self, branch: str) -> str:
        """Return the S3 key for *branch* as `{base_key}/{branch}/.testmondata`."""
        safe = "/".join(part for part in branch.replace("\\", "/").split("/") if part)
        return f"{self._key}/{safe}/.testmondata" if safe else f"{self._key}/.testmondata"

    def _download_to(self, path: str, key: str | None = None) -> str | None:
        """Download the S3 object at *key* to *path*. Returns ETag or None if missing."""
        key = key if key is not None else self._key
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=key)
            with open(path, "wb") as f:
                f.write(response["Body"].read())
            return response["ETag"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
