def _build_model_input(question: str, context_text: str) -> str:
    base_question = (question or "").strip()
    normalized_context = (context_text or "").strip()
    if not normalized_context:
        return base_question
    return (
        "Thread context (older -> newer):\n"
        f"{normalized_context}\n\n"
        "Current user question:\n"
        f"{base_question}"
    )
