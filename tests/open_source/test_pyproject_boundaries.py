import json
import tomllib
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_toml(path: Path) -> dict:
    with path.open("rb") as fp:
        return tomllib.load(fp)


class PyprojectBoundaryTests(unittest.TestCase):
    def test_root_pyproject_includes_only_open_core_package(self) -> None:
        data = _load_toml(PROJECT_ROOT / "pyproject.toml")
        include = (((data.get("tool") or {}).get("setuptools") or {}).get("packages") or {}).get("find", {}).get(
            "include",
            [],
        )
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        optional_dependencies = project.get("optional-dependencies", {})

        self.assertEqual(include, ["boxer", "boxer.*"])
        self.assertEqual(
            dependencies,
            [
                "python-dotenv==1.1.1",
                "anthropic==0.74.1",
            ],
        )
        self.assertEqual(
            optional_dependencies,
            {
                "db": ["pymysql==1.1.1"],
                "s3": ["boto3==1.34.162"],
                "all": [
                    "pymysql==1.1.1",
                    "boto3==1.34.162",
                ],
            },
        )
        self.assertNotIn("scripts", project)

    def test_public_slack_pyproject_is_separate_install_unit(self) -> None:
        data = _load_toml(PROJECT_ROOT / "boxer_adapter_slack" / "pyproject.toml")
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])

        self.assertEqual(project.get("name"), "boxer-adapter-slack")
        self.assertIn("boxer>=0.1.0", dependencies)
        self.assertIn("slack-bolt==1.26.0", dependencies)
        self.assertEqual(packages, ["boxer_adapter_slack"])

    def test_public_web_pyproject_is_separate_install_unit(self) -> None:
        data = _load_toml(PROJECT_ROOT / "boxer_adapter_web" / "pyproject.toml")
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])
        scripts = project.get("scripts", {})

        self.assertEqual(project.get("name"), "boxer-adapter-web")
        self.assertIn("boxer>=0.1.0", dependencies)
        self.assertIn("fastapi==0.116.1", dependencies)
        self.assertIn("websockets==15.0.1", dependencies)
        self.assertEqual(packages, ["boxer_adapter_web"])
        self.assertEqual(scripts["boxer-web"], "boxer_adapter_web.runtime:main")
        self.assertEqual(
            scripts["boxer-web-bootstrap-admin"],
            "boxer_adapter_web.bootstrap_admin:main",
        )

    def test_widget_package_excludes_admin_runtime(self) -> None:
        package_data = json.loads((PROJECT_ROOT / "widget" / "package.json").read_text(encoding="utf-8"))

        # 설치 대상 서비스에는 SDK/widget만 들어가고 React/admin은 runtime dependency가 아니다.
        self.assertEqual(package_data["files"], ["dist/sdk", "dist/widget"])
        self.assertNotIn("dependencies", package_data)
        self.assertIn("build:admin", package_data["scripts"])

    def test_legacy_company_subproject_directory_is_removed(self) -> None:
        self.assertFalse((PROJECT_ROOT / "company" / "pyproject.toml").exists())

    def test_root_requirements_alias_files_are_removed(self) -> None:
        for relative_path in (
            "requirements.txt",
            "requirements-open-core.txt",
            "requirements-slack.txt",
            "requirements-company.txt",
            "boxer/requirements.txt",
            "boxer_adapter_slack/requirements.txt",
        ):
            self.assertFalse((PROJECT_ROOT / relative_path).exists(), relative_path)

    def test_open_core_layout_uses_rag_named_directories(self) -> None:
        self.assertTrue((PROJECT_ROOT / "boxer" / "core" / "settings.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer" / "context" / "entries.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer" / "context" / "builder.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer" / "context" / "windowing.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer" / "observability" / "request_log.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer" / "retrieval" / "synthesis.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer" / "retrieval" / "knowledge.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer" / "retrieval" / "connectors" / "db.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer_adapter_slack" / "context.py").exists())
        self.assertTrue((PROJECT_ROOT / "boxer_adapter_web" / "app.py").exists())
        self.assertFalse((PROJECT_ROOT / "boxer" / "routers" / "common").exists())
        self.assertFalse((PROJECT_ROOT / "boxer" / "context" / "thread_context.py").exists())
        self.assertFalse((PROJECT_ROOT / "boxer" / "context" / "models.py").exists())
        self.assertFalse((PROJECT_ROOT / "boxer" / "retrieval" / "connectors" / "request_log.py").exists())
        self.assertFalse((PROJECT_ROOT / "boxer" / "observability" / "request_log_backup.py").exists())
        self.assertFalse((PROJECT_ROOT / "boxer" / "observability" / "request_audit_backup.py").exists())


if __name__ == "__main__":
    unittest.main()
