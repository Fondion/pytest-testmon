import os

import pytest

pytest.importorskip("boto3")

from botocore.exceptions import ClientError  # noqa: E402

from testmon import db as testmon_db  # noqa: E402
from testmon import storage_s3  # noqa: E402
from testmon.storage_s3 import S3Storage  # noqa: E402
from testmon import testmon_core  # noqa: E402

pytest_plugins = ("pytester",)

BUCKET = "bucket"
S3_URL = f"s3://{BUCKET}/testmon"
ENV = "default"
PY = "3.13.1"
OLD_PACKAGES = "requests 2.31"
NEW_PACKAGES = "requests 2.32"
TEST_NAME = "test_a.py::test_a"


class FakeS3:
    """In-memory stand-in for the boto3 S3 client calls S3Storage makes."""

    def __init__(self):
        self.objects = {}
        self.gets = []
        self.puts = []

    def get_object(self, Bucket, Key):  # pylint: disable=invalid-name
        self.gets.append(Key)
        if (Bucket, Key) not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
            )
        data = self.objects[(Bucket, Key)]

        class _Body:
            def read(self):
                return data

        return {"Body": _Body(), "ETag": f'"{hash(data)}"'}

    def put_object(self, Bucket, Key, Body, **_):  # pylint: disable=invalid-name
        self.puts.append(Key)
        self.objects[(Bucket, Key)] = Body


@pytest.fixture
def fake_s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(storage_s3.boto3, "client", lambda *a, **kw: fake)
    for var in (
        "TESTMON_FALLBACK_BRANCH",
        "GITHUB_BASE_REF",
        "CI_MERGE_REQUEST_TARGET_BRANCH_NAME",
        "BITBUCKET_PR_DESTINATION_BRANCH",
        "CHANGE_TARGET",
    ):
        monkeypatch.delenv(var, raising=False)
    return fake


def put_branch_object(fake, tmp_path, branch, packages, tests=(TEST_NAME,)):
    """Store an S3 object for *branch* holding one environment with *tests*."""
    path = tmp_path / f"{branch}.s3object"
    database = testmon_db.DB(str(path))
    exec_id, _ = database.fetch_or_create_environment(ENV, packages, PY, branch)
    database.insert_test_file_fps(
        {
            name: {
                "deps": [
                    {
                        "filename": "test_a.py",
                        "fsha": "abc",
                        "method_checksums": [1, 2],
                    }
                ],
                "failed": False,
                "duration": 0.1,
            }
            for name in tests
        },
        exec_id,
    )
    database.con.close()
    fake.objects[(BUCKET, f"testmon/{branch}/.testmondata")] = path.read_bytes()


def test_branch_object_with_old_packages_seeds_from_fallback(fake_s3, tmp_path):
    # The branch last ran before master bumped its requirements; master has
    # already run with the new packages.
    put_branch_object(fake_s3, tmp_path, "feature", OLD_PACKAGES)
    put_branch_object(fake_s3, tmp_path, "master", NEW_PACKAGES)

    storage = S3Storage(S3_URL, readonly=False, fallback_branch="master")
    database = storage.setup(
        local_db_path=str(tmp_path / ".testmondata"),
        env_name=ENV,
        system_packages=NEW_PACKAGES,
        python_version=PY,
        branch="feature",
    )

    assert storage.seed_from_fallback(ENV, NEW_PACKAGES, PY, "feature")
    result = database.initiate_execution(ENV, NEW_PACKAGES, PY, {}, branch="feature")
    assert result["packages_changed"] is False
    assert TEST_NAME in database.all_test_executions(result["exec_id"])
    storage.cleanup()


def test_branch_object_with_current_packages_skips_fallback(fake_s3, tmp_path):
    put_branch_object(fake_s3, tmp_path, "feature", NEW_PACKAGES)
    put_branch_object(fake_s3, tmp_path, "master", NEW_PACKAGES)

    storage = S3Storage(S3_URL, readonly=False, fallback_branch="master")
    storage.setup(
        local_db_path=str(tmp_path / ".testmondata"),
        env_name=ENV,
        system_packages=NEW_PACKAGES,
        python_version=PY,
        branch="feature",
    )

    assert fake_s3.gets == ["testmon/feature/.testmondata"]
    storage.cleanup()


def test_read_branch_uses_snapshot_without_seed_or_upload(fake_s3, tmp_path):
    put_branch_object(fake_s3, tmp_path, "feature", NEW_PACKAGES, tests=("x::y",))
    put_branch_object(fake_s3, tmp_path, "master", NEW_PACKAGES)
    local_db_path = tmp_path / ".testmondata"

    storage = S3Storage(
        S3_URL, readonly=False, fallback_branch="master", read_branch="master"
    )
    database = storage.setup(
        local_db_path=str(local_db_path),
        env_name=ENV,
        system_packages=NEW_PACKAGES,
        python_version=PY,
        branch="master",
        target_branch="master",
    )

    assert fake_s3.gets == ["testmon/master/.testmondata"]
    assert not local_db_path.exists()
    assert storage.readonly
    assert not storage.seed_from_fallback(ENV, NEW_PACKAGES, PY, "master")
    result = database.initiate_execution(ENV, NEW_PACKAGES, PY, {}, branch="master")
    assert result["packages_changed"] is False
    assert TEST_NAME in database.all_test_executions(result["exec_id"])

    storage.merge_and_upload(
        {"new.py::test": {"deps": [], "failed": False, "duration": 0.1}},
        ENV,
        NEW_PACKAGES,
        PY,
        "master",
    )
    assert not fake_s3.puts

    snapshot_path = storage._local_db_path  # pylint: disable=protected-access
    storage.cleanup()
    assert not os.path.exists(snapshot_path)


def test_read_branch_missing_object_is_an_error(fake_s3, tmp_path):
    storage = S3Storage(S3_URL, read_branch="master")
    with pytest.raises(testmon_core.TestmonException, match="has no S3 object"):
        storage.setup(
            local_db_path=str(tmp_path / ".testmondata"),
            env_name=ENV,
            system_packages=NEW_PACKAGES,
            python_version=PY,
            branch="master",
        )


def test_read_branch_plugin_run(fake_s3, pytester):
    pytester.makepyfile(test_a="def test_a():\n    assert True\n")
    s3_args = (f"--testmon-s3={S3_URL}",)

    # master run collects and uploads the snapshot
    pytester.runpytest_inprocess(*s3_args, "--testmon-s3-branch=master")
    assert fake_s3.puts == ["testmon/master/.testmondata"]

    # a PR run against master's snapshot: known unchanged test is deselected,
    # a brand-new test file runs, nothing is uploaded
    os.remove(pytester.path / ".testmondata")
    pytester.makepyfile(test_b="def test_b():\n    assert True\n")
    result = pytester.runpytest_inprocess(
        *s3_args,
        "--testmon-s3-branch=feature",
        "--testmon-s3-read-branch=master",
        "--testmon-nocollect",
        "-v",
    )
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*test_b.py::test_b PASSED*"])
    result.stdout.no_fnmatch_line("*test_a.py::test_a*")
    assert fake_s3.puts == ["testmon/master/.testmondata"]
    assert not (pytester.path / ".testmondata").exists()
