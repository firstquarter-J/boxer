import tomllib
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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

        self.assertEqual(include, ["boxer", "boxer.*"])
        self.assertNotIn("optional-dependencies", project)
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

    def test_company_domain_pyproject_is_separate_install_unit(self) -> None:
        data = _load_toml(PROJECT_ROOT / "boxer_company" / "pyproject.toml")
        project = data.get("project") or {}
        dependencies = project.get("dependencies", [])
        packages = ((data.get("tool") or {}).get("setuptools") or {}).get("packages", [])

        self.assertEqual(project.get("name"), "boxer-company")
        self.assertIn("boxer>=0.1.0", dependencies)
        self.assertEqual(packages, ["boxer_company", "boxer_company.routers"])
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
            "boxer_company/requirements.txt",
            "boxer_company_adapter_slack/requirements.txt",
        ):
            self.assertFalse((PROJECT_ROOT / relative_path).exists(), relative_path)


if __name__ == "__main__":
    unittest.main()
