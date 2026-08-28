import ast
import subprocess
import sys
import tomllib
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_toml(path: Path) -> dict:
    with path.open("rb") as fp:
        return tomllib.load(fp)


def _load_import_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def _assert_forbidden_imports_absent(
    testcase: unittest.TestCase,
    root: Path,
    forbidden_imports: tuple[str, ...],
) -> None:
    for path in root.rglob("*.py"):
        relative_parts = path.relative_to(root).parts
        # editable/build 결과물은 source import 경계가 아니며 과거 복사본을
        # 포함할 수 있으므로 실제 package source만 검사한다.
        if any(
            part in {"build", "dist", "__pycache__"}
            or part.endswith(".egg-info")
            for part in relative_parts
        ):
            continue
        imported_names = _load_import_names(path)
        for forbidden_import in forbidden_imports:
            testcase.assertFalse(
                any(
                    imported_name == forbidden_import
                    or imported_name.startswith(f"{forbidden_import}.")
                    for imported_name in imported_names
                ),
                f"{path}: {forbidden_import}",
            )


class CompanyPackagingBoundaryTests(unittest.TestCase):
    def test_company_domain_pyproject_is_separate_install_unit(self) -> None:
        data = _load_toml(PROJECT_ROOT / "boxer_company" / "pyproject.toml")
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        optional_dependencies = project.get("optional-dependencies", {})
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])
        scripts = project.get("scripts", {})

        self.assertEqual(project.get("name"), "boxer-company")
        self.assertEqual(dependencies, ["boxer>=0.1.0"])
        # Slack transport는 base 계약만 설치하고, DB/S3/SSH 등 provider는
        # 실제 도메인을 실행하는 API install unit만 runtime extra로 고른다.
        self.assertEqual(
            optional_dependencies.get("runtime"),
            [
                "boxer[db,s3]>=0.1.0",
                "cryptography>=43,<49",
                "google-auth>=2.38,<3",
                "paramiko==3.5.1",
                "redis==5.0.8",
                "requests==2.32.3",
            ],
        )
        self.assertEqual(
            packages,
            [
                "boxer_company",
                "boxer_company.assets",
                "boxer_company.assistant",
                "boxer_company.routers",
            ],
        )
        self.assertNotIn("boxer", packages)
        self.assertNotIn("boxer_adapter_slack", packages)
        self.assertEqual(
            scripts,
            {
                "boxer-company-base-access-seed": (
                    "boxer_company.base_access_seed:main"
                ),
                # Slack/API 모두가 설치하는 회사 domain CLI로
                # 각 host의 canonical SMS 상태를 create-only 초기화한다.
                "boxer-company-sms-recovery-state-init": (
                    "boxer_company.sms_recovery_state_initializer:main"
                ),
            },
        )

    def test_company_slack_pyproject_depends_on_public_and_company_layers(self) -> None:
        data = _load_toml(PROJECT_ROOT / "boxer_company_adapter_slack" / "pyproject.toml")
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])
        scripts = project.get("scripts", {})

        self.assertEqual(project.get("name"), "boxer-company-adapter-slack")
        self.assertEqual(packages, ["boxer_company_adapter_slack"])
        self.assertEqual(
            dependencies,
            [
                "boxer-adapter-slack>=0.1.0",
                "boxer-company>=0.1.0",
                "requests==2.32.3",
            ],
        )
        self.assertNotIn("boxer-company[runtime]>=0.1.0", dependencies)
        self.assertFalse(
            any(
                dependency.startswith(
                    (
                        "boto3",
                        "cryptography",
                        "google-auth",
                        "paramiko",
                        "pymysql",
                        "redis",
                    )
                )
                for dependency in dependencies
            )
        )
        # 자동화 domain state는 API가 소유하므로 Slack 설치 단위에는
        # 과거 forward migration CLI를 다시 노출하지 않는다.
        self.assertEqual(scripts, {})

    def test_company_slack_package_import_keeps_entrypoint_lazy(self) -> None:
        script = """
import sys
import boxer_company_adapter_slack

assert "boxer_company_adapter_slack.company" not in sys.modules
for prefix in ("pymysql", "boto3", "botocore", "paramiko", "anthropic", "redis"):
    assert not any(
        name == prefix or name.startswith(prefix + ".")
        for name in sys.modules
    ), prefix
"""
        # DTO/client submodule 소비가 create_app 조립과 runtime provider 설치를
        # 암묵적으로 요구하지 않도록 깨끗한 interpreter에서 확인한다.
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_company_slack_transport_imports_need_no_runtime_providers(self) -> None:
        script = """
import sys
import boxer_company_adapter_slack.automation_api_client
import boxer_company_adapter_slack.automation_reporter
import boxer_company_adapter_slack.daily_device_round_reporter
import boxer_company_adapter_slack.device_health_monitor_reporter
import boxer_company_adapter_slack.device_notification_alert_reporter
import boxer_company_adapter_slack.weekly_recordings_reporter
import boxer_company_adapter_slack.hpa_change_api_client
import boxer_company_adapter_slack.hpa_change_remote_reporter

assert "boxer_company_adapter_slack.company" not in sys.modules
assert "boxer_company.automation" not in sys.modules
for prefix in (
    "pymysql",
    "boto3",
    "botocore",
    "paramiko",
    "anthropic",
    "redis",
    "gspread",
):
    assert not any(
        name == prefix or name.startswith(prefix + ".")
        for name in sys.modules
    ), prefix
"""
        # Slack delivery pull/ACK와 renderer는 company runtime extra 없이
        # 독립 설치 가능한 transport 계약이어야 한다.
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_company_slack_source_has_no_domain_runtime_imports(self) -> None:
        adapter_root = PROJECT_ROOT / "boxer_company_adapter_slack"

        # Slack에는 provider-free matcher/DTO만 허용하고 과거 domain cycle,
        # DB/S3/MDA/LLM/HPA workflow 실행 모듈이 다시 들어오지 못하게 한다.
        _assert_forbidden_imports_absent(
            self,
            adapter_root,
            (
                "anthropic",
                "boto3",
                "botocore",
                "gspread",
                "paramiko",
                "pymysql",
                "redis",
                "boxer_company.automation",
                "boxer_company.daily_device_round",
                "boxer_company.device_health_monitor_cycle",
                "boxer_company.device_notification_cycle",
                "boxer_company.hpa_change_coordinator",
                "boxer_company.hpa_change_workflow",
                "boxer_company.notion_workspace_search",
                "boxer_company.sms_delivery",
                "boxer_company.sms_delivery_cycle",
                "boxer_company.thread_playbook_learning",
                "boxer_company.weekly_recordings_report",
                "boxer_company.assistant.factory",
                "boxer_company.assistant.operations",
                "boxer_company.assistant.runtime",
            ),
        )

    def test_company_slack_entry_imports_no_execution_provider(self) -> None:
        script = """
import sys
import boxer_company_adapter_slack.company

assert "boxer_company.read_routing" in sys.modules
for prefix in (
    "pymysql",
    "boto3",
    "botocore",
    "paramiko",
    "anthropic",
    "gspread",
    "redis",
    "google.auth",
    "cryptography",
    "boxer_company.automation",
    "boxer_company.daily_device_round",
    "boxer_company.device_health_monitor_cycle",
    "boxer_company.device_notification_cycle",
    "boxer_company.sms_delivery",
    "boxer_company.weekly_recordings_report",
    "boxer_company.notion_workspace_search",
    "boxer_company.thread_playbook_learning",
    "boxer_company.hpa_change_workflow",
    "boxer_company.hpa_change_coordinator",
    "boxer_company.assistant.runtime",
    "boxer_company.assistant.operations",
):
    assert not any(
        name == prefix or name.startswith(prefix + ".")
        for name in sys.modules
    ), prefix
"""
        # production entry 전체를 조립해도 Slack SDK/API client/renderer 외
        # 실행 provider나 회사 domain runtime은 import되지 않아야 한다.
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
            env={"BOXER_SKIP_DOTENV": "true"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_company_api_pyproject_is_private_install_unit(self) -> None:
        data = _load_toml(PROJECT_ROOT / "boxer_company_api" / "pyproject.toml")
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])
        scripts = project.get("scripts", {})

        self.assertEqual(project.get("name"), "boxer-company-api")
        self.assertEqual(project.get("requires-python"), ">=3.11")
        self.assertEqual(
            dependencies,
            [
                "boxer-company[runtime]>=0.1.0",
                "fastapi==0.116.1",
                "pydantic>=2.11,<3",
                "uvicorn==0.35.0",
            ],
        )
        self.assertEqual(packages, ["boxer_company_api"])
        self.assertEqual(
            scripts,
            {
                "boxer-company-api": (
                    "boxer_company_api.runtime:main"
                ),
                "boxer-company-api-automation": (
                    "boxer_company_api.automation_scheduler:main"
                ),
                "boxer-company-api-automation-resolve": (
                    "boxer_company_api.automation_recovery:main"
                ),
                "boxer-company-api-hpa-coordinator": (
                    "boxer_company_api.hpa_change_coordinator_runner:main"
                ),
            },
        )

    def test_company_requirements_alias_files_are_removed(self) -> None:
        for relative_path in (
            "boxer_company/requirements.txt",
            "boxer_company_adapter_slack/requirements.txt",
            "boxer_company_api/requirements.txt",
        ):
            self.assertFalse((PROJECT_ROOT / relative_path).exists(), relative_path)

    def test_company_assistant_package_has_no_adapter_runtime_imports(self) -> None:
        assistant_root = PROJECT_ROOT / "boxer_company" / "assistant"

        # 공통 factory를 어느 adapter에서도 재사용하도록 transport 의존 역류를 막는다.
        _assert_forbidden_imports_absent(
            self,
            assistant_root,
            (
                "boxer_adapter_slack",
                "boxer_company_adapter_slack",
                "boxer_adapter_web",
                "slack_bolt",
            ),
        )

    def test_company_api_package_has_no_slack_or_web_adapter_imports(self) -> None:
        api_root = PROJECT_ROOT / "boxer_company_api"

        # 내부 HTTP 서버는 회사 service와 공개 core만 조립하고 adapter를 우회하지 않는다.
        _assert_forbidden_imports_absent(
            self,
            api_root,
            (
                "boxer_adapter_slack",
                "boxer_company_adapter_slack",
                "boxer_adapter_web",
                "slack_bolt",
            ),
        )


if __name__ == "__main__":
    unittest.main()
