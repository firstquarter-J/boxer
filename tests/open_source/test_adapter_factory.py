import unittest

from boxer_adapter_slack.factory import load_entrypoint


class AdapterFactoryCompatibilityTests(unittest.TestCase):
    def test_keeps_sample_entrypoint_in_open_adapter(self) -> None:
        factory = load_entrypoint("boxer_adapter_slack.sample:create_app")

        self.assertEqual(factory.__module__, "boxer_adapter_slack.sample")

    def test_keeps_legacy_public_alias_pointing_to_sample_adapter(self) -> None:
        factory = load_entrypoint("boxer.adapters.slack:create_app")

        self.assertEqual(factory.__module__, "boxer_adapter_slack.sample")

    def test_rejects_invalid_entrypoint_format(self) -> None:
        with self.assertRaises(RuntimeError):
            load_entrypoint("not-a-valid-entrypoint")


if __name__ == "__main__":
    unittest.main()
