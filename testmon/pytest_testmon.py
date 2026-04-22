# -*- coding: utf-8 -*-
"""
Main module of testmon pytest plugin.
"""

import time
import xmlrpc.client
import os

from collections import defaultdict
from datetime import date, timedelta

from pathlib import Path
import pytest

from _pytest.config import ExitCode, Config
from _pytest.terminal import TerminalReporter

from testmon.configure import TmConf

from testmon.testmon_core import (
    TestmonCollector,
    eval_environment,
    TestmonData,
    home_file,
    TestmonException,
    get_test_execution_class_name,
    get_test_execution_module_name,
    cached_relpath,
)
from testmon import configure
from testmon.common import get_logger, get_system_packages, git_current_branch

SURVEY_NOTIFICATION_INTERVAL = timedelta(days=28)

logger = get_logger(__name__)


def pytest_addoption(parser):
    group = parser.getgroup(
        "automatically select tests affected by changes (pytest-testmon)"
    )

    group.addoption(
        "--testmon",
        action="store_true",
        dest="testmon",
        help=(
            "Select tests affected by changes (based on previously collected data) "
            "and collect + write new data (.testmondata file). "
            "Either collection or selection might be deactivated "
            "(sometimes automatically). See below."
        ),
    )

    group.addoption(
        "--testmon-noselect",
        action="store_true",
        dest="testmon_noselect",
        help=(
            "Reorder and prioritize the tests most likely to fail first, but don't deselect anything. "
            "Forced if you use -m, -k, -l, -lf, test_file.py::test_name"
        ),
    )

    group.addoption(
        "--testmon-nocollect",
        action="store_true",
        dest="testmon_nocollect",
        help=(
            "Run testmon but deactivate the collection and writing of testmon data. "
            "Forced if you run under debugger or coverage."
        ),
    )

    group.addoption(
        "--testmon-forceselect",
        action="store_true",
        dest="testmon_forceselect",
        help=(
            "Run testmon and select only tests affected by changes "
            "and satisfying pytest selectors at the same time."
        ),
    )

    group.addoption(
        "--no-testmon",
        action="store_true",
        dest="no-testmon",
        help=(
            "Turn off (even if activated from config by default).\n"
            "Forced if neither read nor write is possible "
            "(debugger plus test selector)."
        ),
    )

    group.addoption(
        "--testmon-env",
        action="store",
        type=str,
        dest="environment_expression",
        default="",
        help=(
            "This allows you to have separate coverage data within one"
            " .testmondata file, e.g. when using the same source"
            " code serving different endpoints or Django settings."
        ),
    )

    group.addoption(
        "--tmnet",
        action="store_true",
        dest="tmnet",
        help=(
            "Use tmnet cloud for storage instead of .testmondata file used by --testmon (see https://www.testmon.net)."
        ),
    )

    group.addoption(
        "--testmon-s3",
        action="store",
        dest="testmon_s3",
        default=None,
        metavar="S3_URL",
        help=(
            "S3 URL (s3://bucket/key) for shared SQLite cache. "
            "Downloads at session start; read-only unless --testmon-s3-write is also given."
        ),
    )

    group.addoption(
        "--testmon-s3-write",
        action="store_true",
        dest="testmon_s3_write",
        help="Merge this run's results back to S3 at session end (requires --testmon-s3).",
    )

    group.addoption(
        "--testmon-s3-force-pull",
        action="store_true",
        dest="testmon_s3_force_pull",
        help=(
            "Always re-download the S3 cache even when the local DB already has "
            "data for the current branch."
        ),
    )

    group.addoption(
        "--testmon-s3-branch",
        action="store",
        dest="testmon_s3_branch",
        default=None,
        metavar="BRANCH",
        help=(
            "Branch name used as a cache key. "
            "Auto-detected from git/CI env vars when omitted. "
            "Pass an empty string to disable branch discrimination."
        ),
    )

    parser.addini("environment_expression", "environment expression", default="")
    parser.addini(
        "testmon_ignore_dependencies",
        "ignore dependencies",
        type="args",
        default=[],
    )
    parser.addini("tmnet_url", "URL of the testmon.net api server.")
    parser.addini("tmnet_api_key", "testmon api key")
    parser.addini(
        "testmon_s3_url", "S3 URL for shared testmon cache (s3://bucket/key)."
    )
    parser.addini(
        "testmon_s3_fallback_branch",
        "Branch to seed from when current branch has no S3 cache (default: main).",
        default="main",
    )


def testmon_options(config):
    result = []
    for label in [
        "testmon",
        "no-testmon",
        "environment_expression",
    ]:
        if config.getoption(label):
            result.append(label.replace("testmon_", ""))
    return result


def _resolve_branch(config: Config) -> str:
    override = config.getoption("testmon_s3_branch")
    if override is not None:
        return override  # empty string explicitly disables branch discrimination
    return git_current_branch() or ""


def init_testmon_data(config: Config):
    environment = config.getoption("environment_expression") or eval_environment(
        config.getini("environment_expression")
    )
    ignore_dependencies = config.getini("testmon_ignore_dependencies")
    system_packages = get_system_packages(ignore=ignore_dependencies)
    branch = _resolve_branch(config)

    # --- legacy tmnet proxy ---
    url = config.getini("tmnet_url")
    rpc_proxy = None

    if config.testmon_config.tmnet or getattr(config, "tmnet", None):
        rpc_proxy = getattr(config, "tmnet", None)

        if not url:
            url = "https://api1.testmon.net/"
        if not rpc_proxy:
            tmnet_api_key = config.getini("tmnet_api_key")
            if "TMNET_API_KEY" in os.environ:
                if tmnet_api_key:
                    logger.warning(
                        "Duplicate TMNET_API_KEY (environment and ini file). \
                         Using TMNET_API_KEY from %s",
                        config.inipath,
                    )
                else:
                    tmnet_api_key = os.getenv("TMNET_API_KEY")

            if not tmnet_api_key.strip():
                raise ValueError(
                    "TMNET_API_KEY is required when using --tmnet. "
                    "Please set it in pytest.ini, pyproject.toml, or as an environment variable. "
                )

            rpc_proxy = xmlrpc.client.ServerProxy(
                url,
                allow_none=True,
                headers=[("x-api-key", tmnet_api_key.strip())],
            )

    # --- S3 storage ---
    database = rpc_proxy  # may be overridden by S3 below
    s3_url = config.getoption("testmon_s3") or config.getini("testmon_s3_url")

    running_as = get_running_as(config)

    if s3_url and running_as != "worker":
        import sys as _sys

        from testmon.storage_s3 import S3Storage
        from testmon.common import drop_patch_version

        fallback_branch = config.getini("testmon_s3_fallback_branch") or "main"
        readonly = not config.getoption("testmon_s3_write")
        force_pull = config.getoption("testmon_s3_force_pull")
        s3 = S3Storage(s3_url, readonly=readonly, fallback_branch=fallback_branch)
        database = s3.setup(force_pull=force_pull)

        # Seed branch data from fallback before initiate_execution so the
        # seeded environment row is found rather than created empty.
        env_name = environment if environment else "default"
        pkg_str = drop_patch_version(system_packages)
        py_str = f"{_sys.version_info.major}.{_sys.version_info.minor}.{_sys.version_info.micro}"
        s3.seed_from_fallback(env_name, pkg_str, py_str, branch)

        config._testmon_s3 = s3

    # --- xdist worker input ---
    exec_id = None
    system_packages_change = None
    files_of_interest = None

    if running_as == "worker" and hasattr(config, "workerinput"):
        exec_id = config.workerinput.get("testmon_exec_id")
        system_packages_change = config.workerinput.get(
            "testmon_system_packages_change"
        )
        files_of_interest = config.workerinput.get("testmon_files_of_interest")

        # If the controller used an S3 temp file, open a readonly connection to it.
        s3_db_path = config.workerinput.get("testmon_s3_db_path")
        if s3_db_path and os.path.exists(s3_db_path):
            from testmon import db as _db

            database = _db.DB(s3_db_path, readonly=True)

    if running_as == "worker" and exec_id is not None:
        testmon_data: TestmonData = TestmonData.for_worker(
            rootdir=config.rootdir.strpath,
            exec_id=exec_id,
            database=database,
            system_packages_change=system_packages_change,
            files_of_interest=files_of_interest,
            environment=environment,
        )
    else:
        testmon_data: TestmonData = TestmonData.for_local_run(
            rootdir=config.rootdir.strpath,
            database=database,
            environment=environment,
            system_packages=system_packages,
            branch=branch,
        )

    testmon_data.determine_stable()
    config.testmon_data = testmon_data


def get_running_as(config):
    if hasattr(config, "workerinput"):
        return "worker"

    if getattr(config.option, "dist", "no") == "no":
        return "single"

    return "controller"


def register_plugins(config, should_select, should_collect, cov_plugin):
    if should_select or should_collect:
        config.pluginmanager.register(
            TestmonSelect(config, config.testmon_data), "TestmonSelect"
        )

    if should_collect:
        config.pluginmanager.register(
            TestmonCollect(
                TestmonCollector(
                    config.rootdir.strpath,
                    testmon_labels=testmon_options(config),
                    cov_plugin=cov_plugin,
                ),
                config.testmon_data,
                running_as=get_running_as(config),
            ),
            "TestmonCollect",
        )
        if config.pluginmanager.hasplugin("xdist"):
            config.pluginmanager.register(TestmonXdistSync())


def pytest_configure(config):
    coverage_stack = None
    try:
        from tmnet.testmon_core import (  # pylint: disable=import-outside-toplevel
            Testmon as UberTestmon,
        )

        coverage_stack = UberTestmon.coverage_stack
    except ImportError:
        pass

    cov_plugin = None
    cov_plugin = config.pluginmanager.get_plugin("_cov")

    tm_conf = configure.header_collect_select(
        config, coverage_stack, cov_plugin=cov_plugin
    )
    config.testmon_config: TmConf = tm_conf
    if tm_conf.select or tm_conf.collect:
        try:
            init_testmon_data(config)
            register_plugins(config, tm_conf.select, tm_conf.collect, cov_plugin)
        except TestmonException as error:
            pytest.exit(str(error))


def pytest_report_header(config):
    tm_conf = config.testmon_config

    if tm_conf.collect or tm_conf.select:
        unstable_files = getattr(config.testmon_data, "unstable_files", set())
        stable_files = getattr(config.testmon_data, "stable_files", set())
        environment = config.testmon_data.environment

        tm_conf.message += changed_message(
            config,
            environment,
            config.testmon_data.system_packages_change,
            tm_conf.select,
            stable_files,
            unstable_files,
        )

        show_survey_notification = True
        last_notification_date = config.testmon_data.db.fetch_attribute(
            "last_survey_notification_date"
        )
        if last_notification_date:
            last_notification_date = date.fromisoformat(last_notification_date)
            if date.today() - last_notification_date < SURVEY_NOTIFICATION_INTERVAL:
                show_survey_notification = False
            else:
                config.testmon_data.db.write_attribute(
                    "last_survey_notification_date", date.today().isoformat()
                )
        else:
            config.testmon_data.db.write_attribute(
                "last_survey_notification_date", date.today().isoformat()
            )

        if show_survey_notification:
            tm_conf.message += (
                "\nWe'd like to hear from testmon users! "
                "Please go to https://testmon.org/survey to leave feedback."
            )
    return tm_conf.message


def changed_message(
    config,
    environment,
    packages_change,
    should_select,
    stable_files,
    unstable_files,
):
    message = ""
    if should_select:
        changed_files_msg = ", ".join(unstable_files)
        if changed_files_msg == "" or len(changed_files_msg) > 100:
            changed_files_msg = str(len(config.testmon_data.unstable_files))

        if config.testmon_data.new_db:
            message += "new DB, "
        else:
            message += (
                "The packages installed in your Python environment have been changed. "
                "All tests have to be re-executed. "
                if packages_change
                else f"changed files: {changed_files_msg}, unchanged files: {len(stable_files)}, "
            )
    if config.testmon_data.environment:
        branch = getattr(config.testmon_data, "branch", "")
        message += f"environment: {environment}, branch: {branch}"
    return message


def pytest_unconfigure(config):
    if hasattr(config, "testmon_data"):
        config.testmon_data.close_connection()


class TestmonCollect:
    def __init__(
        self, testmon, testmon_data: TestmonData, running_as="single", cov_plugin=None
    ):
        self.testmon_data: TestmonData = testmon_data
        self.testmon: TestmonCollector = testmon
        self._running_as = running_as

        self.reports = defaultdict(lambda: {})
        self.raw_test_names = []
        self.cov_plugin = cov_plugin
        self._sessionstarttime = time.time()
        self._delta = {}  # fingerprints written this session, for S3 merge

    @pytest.hookimpl(tryfirst=True, hookwrapper=True)
    def pytest_pycollect_makeitem(self, collector, name, obj):  # pylint: disable=unused-argument
        makeitem_result = yield
        items = makeitem_result.get_result() or []
        try:
            self.raw_test_names.extend(
                [item.nodeid for item in items if isinstance(item, pytest.Item)]
            )
        except TypeError:  # 'Class' object is not iterable
            pass

    @pytest.hookimpl(tryfirst=True)
    def pytest_collection_modifyitems(self, session, config, items):  # pylint: disable=unused-argument
        should_sync = not session.testsfailed and self._running_as in (
            "single",
            "controller",
        )
        if should_sync:
            config.testmon_data.sync_db_fs_tests(retain=set(self.raw_test_names))

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item, nextitem):  # pylint: disable=unused-argument
        self.testmon.start_testmon(item.nodeid, nextitem.nodeid if nextitem else None)
        result = yield
        if result.excinfo and issubclass(result.excinfo[0], BaseException):
            self.testmon.discard_current()

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item, call):  # pylint: disable=unused-argument
        result = yield

        if call.when == "teardown":
            report = result.get_result()
            report.nodes_files_lines = self.testmon.get_batch_coverage_data()
            result.force_result(
                report
            )  # under xdist, report is serialized on the worker and sent to the controller

    @pytest.hookimpl
    def pytest_runtest_logreport(self, report):
        if self._running_as == "worker":
            return

        self.reports[report.nodeid][report.when] = report
        if report.when == "teardown" and hasattr(report, "nodes_files_lines"):
            if report.nodes_files_lines:
                test_executions_fingerprints = self.testmon_data.get_tests_fingerprints(
                    report.nodes_files_lines, self.reports
                )
                self.testmon_data.save_test_execution_file_fps(
                    test_executions_fingerprints
                )
                self._delta.update(test_executions_fingerprints)

    def pytest_keyboard_interrupt(self, excinfo):  # pylint: disable=unused-argument
        if self._running_as == "single":
            nodes_files_lines = self.testmon.get_batch_coverage_data()

            test_executions_fingerprints = self.testmon_data.get_tests_fingerprints(
                nodes_files_lines, self.reports
            )
            self.testmon_data.save_test_execution_file_fps(test_executions_fingerprints)
            self.testmon.close()

    def pytest_sessionfinish(self, session):  # pylint: disable=unused-argument
        if self._running_as in ("single", "controller"):
            self.testmon_data.db.finish_execution(
                self.testmon_data.exec_id,
                time.time() - self._sessionstarttime,
                session.config.testmon_config.select,
            )
            s3 = getattr(session.config, "_testmon_s3", None)
            if s3 is not None:
                if not s3.readonly and self._delta:
                    td = self.testmon_data
                    s3.merge_and_upload(
                        self._delta,
                        td.environment,
                        td.system_packages_str,
                        td.python_version_str,
                        td.branch,
                    )
                    reporter = session.config.pluginmanager.get_plugin(
                        "terminalreporter"
                    )
                    if reporter is not None:
                        reporter.write_line(
                            f"testmon: uploaded results to {s3.s3_url}"
                        )
                s3.cleanup()
        self.testmon.close()


class TestmonXdistSync:
    def __init__(self):
        self.await_nodes = 0

    def pytest_configure_node(self, node):
        """
        Send exec_id and related data from controller to worker during xdist initialization.
        This avoids each worker having to independently determine the environment and check
        for package changes.

        Note: This hook is called on the controller side for each worker node.
        The node.config here is the controller's config, not the worker's config.
        """
        # Verify we're on the controller (not a worker)
        # node.config in this hook is the controller's config
        running_as = get_running_as(node.config)
        if running_as != "controller":
            return  # Safety check: only run on controller

        # Only proceed if testmon_data has been initialized and workerinput exists
        if hasattr(node.config, "testmon_data") and hasattr(node, "workerinput"):
            testmon_data: TestmonData = node.config.testmon_data
            node.workerinput["testmon_exec_id"] = testmon_data.exec_id
            node.workerinput["testmon_system_packages_change"] = (
                testmon_data.system_packages_change
            )
            node.workerinput["testmon_files_of_interest"] = (
                testmon_data.files_of_interest
            )
            s3 = getattr(node.config, "_testmon_s3", None)
            node.workerinput["testmon_s3_db_path"] = (
                s3._local_db_path if s3 is not None else None
            )

    def pytest_testnodeready(self, node):  # pylint: disable=unused-argument
        self.await_nodes += 1

    def pytest_xdist_node_collection_finished(self, node, ids):  # pylint: disable=invalid-name
        self.await_nodes += -1
        if self.await_nodes == 0:
            node.config.testmon_data.sync_db_fs_tests(retain=set(ids))


def did_fail(reports):
    return reports["failed"]


def get_failing(all_test_executions):
    failing_files, failing_tests = set(), {}
    for test_name, result in all_test_executions.items():
        if did_fail(all_test_executions[test_name]):
            failing_files.add(home_file(test_name))
            failing_tests[test_name] = result
    return failing_files, failing_tests


def sort_items_by_duration(items, avg_durations) -> None:
    items.sort(key=lambda item: avg_durations[item.nodeid])
    items.sort(
        key=lambda item: avg_durations[get_test_execution_class_name(item.nodeid)]
    )
    items.sort(
        key=lambda item: avg_durations[get_test_execution_module_name(item.nodeid)]
    )


def format_time_saved(seconds):
    if not seconds:
        seconds = 0
    if seconds >= 3600:
        return f"{int(seconds / 3600)}h {int((seconds % 3600) / 60)}m"
    return f"{int(seconds / 60)}m {int((seconds % 60) % 60)}s"


class TestmonSelect:
    def __init__(self, config, testmon_data: TestmonData):
        self.testmon_data: TestmonData = testmon_data
        self.config = config

        failing_files, failing_test_names = get_failing(testmon_data.all_tests)

        self.deselected_files = [
            file for file in testmon_data.stable_files if file not in failing_files
        ]
        self.deselected_tests = [
            test_name
            for test_name in testmon_data.stable_test_names
            if test_name not in failing_test_names
        ]
        self._interrupted = False

    def pytest_ignore_collect(self, collection_path: Path, config):
        strpath = cached_relpath(str(collection_path), config.rootdir.strpath)
        if strpath in self.deselected_files and self.config.testmon_config.select:
            return True
        return None

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, session, config, items):  # pylint: disable=unused-argument
        selected = []
        deselected = []
        for item in items:
            if item.nodeid in self.deselected_tests:
                deselected.append(item)
            else:
                selected.append(item)

        sort_items_by_duration(selected, self.testmon_data.avg_durations)

        if self.config.testmon_config.select:
            items[:] = selected
            session.config.hook.pytest_deselected(
                items=([FakeItemFromTestmon(session.config)] * len(deselected))
            )
        else:
            sort_items_by_duration(deselected, self.testmon_data.avg_durations)
            items[:] = selected + deselected

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session, exitstatus):
        if len(self.deselected_tests) and exitstatus == ExitCode.NO_TESTS_COLLECTED:
            session.exitstatus = ExitCode.OK

    @pytest.hookimpl(trylast=True)
    def pytest_terminal_summary(self):
        if self._interrupted:
            return

        if not self.config.option.verbose >= 2:
            return

        (
            run_saved_time,
            run_all_time,
            run_saved_tests,
            run_all_tests,
            total_saved_time,
            total_all_time,
            total_saved_tests,
            total_tests_all,
        ) = self.testmon_data.fetch_saving_stats(self.config.testmon_config.select)

        terminal_reporter = TerminalReporter(self.config)
        potential_or_not = ""
        if not self.config.testmon_config.select:
            potential_or_not = "Potential t"
        else:
            potential_or_not = "T"
        terminal_reporter.section(
            f"{potential_or_not}estmon savings (deselected/no testmon)",
            "=",
            **{"blue": True},
        )

        try:
            tests_all_ratio = f"{100.0 * total_saved_tests / total_tests_all:.0f}"
        except ZeroDivisionError:
            tests_all_ratio = "0"
        try:
            tests_current_ratio = f"{100.0 * run_saved_tests / run_all_tests:.0f}"
        except ZeroDivisionError:
            tests_current_ratio = "0"
        msg = f"this run: {run_saved_tests}/{run_all_tests} ({tests_current_ratio}%) tests, "
        msg += format_time_saved(run_saved_time) + "/" + format_time_saved(run_all_time)
        msg += f", all runs: {total_saved_tests}/{total_tests_all} ({tests_all_ratio}%) tests, "
        msg += (
            format_time_saved(total_saved_time)
            + "/"
            + format_time_saved(total_all_time)
        )
        terminal_reporter.write_line(msg)

    def pytest_keyboard_interrupt(self, excinfo):  # pylint: disable=unused-argument
        self._interrupted = True


class FakeItemFromTestmon:  # pylint: disable=too-few-public-methods
    def __init__(self, config):
        self.config = config
