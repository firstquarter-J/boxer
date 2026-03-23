# Custom Adapter Example

이 예제는 `boxer` open core 위에 domain-specific adapter를 붙이는 가장 작은 형태를 보여준다.

포함된 것:

- `adapters/slack.py`: Slack mention handler
- `routers/faq.py`: 아주 단순한 FAQ router

실행 예시:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -e ./boxer_adapter_slack
cp .env.example .env
export ADAPTER_ENTRYPOINT=examples.custom_adapter.adapters.slack:create_app
boxer-slack
```

테스트 질문:

- `@Bot ping`
- `@Bot what is boxer`
- `@Bot how does routing work`
- `@Bot customer email 알려줘`

의도:

- `ping`은 기본 route
- FAQ 질문은 adapter가 router로 넘긴다
- 민감 질문은 adapter의 policy guard가 막는다
