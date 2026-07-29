# Boxer Company Slack Adapter

Company-specific Slack adapter assembly for Boxer.

This package combines:

- `boxer_adapter_slack`
- `boxer_company`

The company Notion read-only route can be migrated to the private company API
with `local`, `shadow`, and `remote` modes. Shadow mode always returns the
existing local result and only compares safe route metadata in the background.
Remote availability fallback is disabled by default and must be enabled
explicitly for the read-only migration window.
