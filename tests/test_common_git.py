import shutil
import subprocess

import pytest

from testmon.common import git_current_branch, git_current_head, git_path

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")

BRANCH_ENV_VARS = (
    "TESTMON_BRANCH",
    "GITHUB_HEAD_REF",
    "GITHUB_REF_NAME",
    "CI_COMMIT_BRANCH",
    "GIT_BRANCH",
    "BRANCH_NAME",
)


def git(cwd, *args):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def main_repo(tmp_path, monkeypatch):
    for var in BRANCH_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    repo = tmp_path / "main"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "master")
    git(
        repo,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@t",
        "commit",
        "-q",
        "--allow-empty",
        "-m",
        "init",
    )
    return repo


def test_main_checkout(main_repo):
    (main_repo / "src").mkdir()
    assert git_current_branch(main_repo / "src") == "master"
    assert git_current_head(main_repo / "src") == git(main_repo, "rev-parse", "HEAD")


@pytest.mark.parametrize("nested", [True, False], ids=["nested", "sibling"])
def test_worktree_returns_its_own_branch(main_repo, tmp_path, nested):
    worktree = main_repo / ".claude" / "worktrees" / "wt" if nested else tmp_path / "wt"
    git(main_repo, "worktree", "add", "-q", "-b", "feature/wt", str(worktree))
    (worktree / "src").mkdir()

    assert (worktree / ".git").is_file()
    assert git_path(worktree / "src").endswith("worktrees/wt")
    assert git_current_branch(worktree / "src") == "feature/wt"
    assert git_current_head(worktree / "src") == git(worktree, "rev-parse", "HEAD")


def test_packed_ref_head(main_repo):
    git(main_repo, "pack-refs", "--all")
    assert git_current_head(main_repo) == git(main_repo, "rev-parse", "HEAD")
