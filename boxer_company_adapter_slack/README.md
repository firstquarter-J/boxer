# Boxer Company Slack Adapter

Company-specific Slack adapter assembly for Boxer.

This package combines:

- `boxer_adapter_slack`
- `boxer_company`

It is a transport-only gateway: it receives Slack events, checks membership,
collects Slack thread context, calls the private company API, renders the
validated response, and journals Slack delivery receipts. Company DB, S3,
Notion, device, provider, HPA, and automation domain work runs only in the
company API. Local execution, shadow comparison, and local fallback are not
runtime modes.
