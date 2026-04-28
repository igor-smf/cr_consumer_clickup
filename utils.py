def _get_push_envelope(req_json: dict) -> dict:
    if not isinstance(req_json, dict) or "message" not in req_json:
        raise ValueError("Invalid Pub/Sub push: missing 'message'")
    return req_json