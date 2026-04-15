import unittest

from boxer_adapter_slack.factory import load_entrypoint


class CompanyAdapterFactoryCompatibilityTests(unittest.TestCase):
    def test_keeps_company_entrypoint_in_company_package(self) -> None:
        factory = load_entrypoint("boxer_company_adapter_slack.company:create_app")

        self.assertEqual(factory.__module__, "boxer_company_adapter_slack.company")

    def test_rejects_removed_legacy_company_entrypoint(self) -> None:
        with self.assertRaises(RuntimeError):
            load_entrypoint("boxer.adapters.company.slack:create_app")


if __name__ == "__main__":
    unittest.main()
