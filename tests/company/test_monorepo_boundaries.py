import ast
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
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])

        self.assertEqual(project.get("name"), "boxer-company")
        self.assertIn("boxer[db,s3]>=0.1.0", dependencies)
        self.assertEqual(
            packages,
            [
                "boxer_company",
                "boxer_company.assistant",
                "boxer_company.routers",
            ],
        )
        self.assertNotIn("boxer", packages)
        self.assertNotIn("boxer_adapter_slack", packages)

    def test_company_slack_pyproject_depends_on_public_and_company_layers(self) -> None:
        data = _load_toml(PROJECT_ROOT / "boxer_company_adapter_slack" / "pyproject.toml")
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])

        self.assertEqual(project.get("name"), "boxer-company-adapter-slack")
        self.assertEqual(packages, ["boxer_company_adapter_slack"])
        self.assertEqual(
            dependencies,
            [
                "boxer-adapter-slack>=0.1.0",
                "boxer-company>=0.1.0",
            ],
        )

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
                "boxer-company>=0.1.0",
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
