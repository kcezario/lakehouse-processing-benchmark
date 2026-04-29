import json

import requests


def submit_spark_code(
    *,
    cfg,
    name,
    code,
    args=None,
    logger=None,
    response_logged_event=None,
):
    url = str(cfg.spark_client_url or "").strip()
    if not url:
        raise ValueError(
            "SPARK_CLIENT_URL is empty. Set it in .env (e.g. "
            "http://localhost:8080/submit), same endpoint that responds to curl quickly."
        )

    payload = {
        "name": name,
        "code": code,
        "args": args or [],
    }

    # Same request contract as examples/dag_teste_spark_inline.py: only X-Tenant.
    # requests define Content-Type: application/json ao usar json=payload.
    headers = {
        "X-Tenant": cfg.tenant,
    }

    connect_s = max(1, int(cfg.spark_connect_timeout_seconds))
    read_http_s = max(1, int(cfg.spark_submit_read_timeout_seconds))
    try:
        payload_bytes = len(json.dumps(payload, default=str).encode("utf-8"))
    except (TypeError, ValueError):
        payload_bytes = -1

    preflight_detail = (
        f"[Spark submit] POST | url={url} | X-Tenant={cfg.tenant} | "
        f"name={name} | payload_bytes≈{payload_bytes} | "
        f"timeout=(connect={connect_s}s, read_http_submit={read_http_s}s)"
    )
    preflight_short = f"[Spark submit] POST | name={name}"
    print(preflight_short, flush=True)
    if logger:
        logger.info(preflight_short)
        logger.debug(preflight_detail)

    if logger:
        logger.debug(f"Submetendo job Spark: {name}")

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=(connect_s, read_http_s),
        )
    except requests.exceptions.ReadTimeout as e:
        raise requests.exceptions.ReadTimeout(
            f"{e} | url={url!r} | read_http_submit={read_http_s}s esgotado sem "
            f"response body. Endpoint may keep the connection open until "
            f"job completion (synchronous API). Increase SPARK_SUBMIT_READ_TIMEOUT_SECONDS "
            f"in .env, or make the service return early (ack) and keep "
            f"progress tracking through Trino polling."
        ) from e
    except requests.exceptions.ConnectTimeout as e:
        raise requests.exceptions.ConnectTimeout(
            f"{e} | url={url!r} | dica: host/porta errados ficam "
            f"waiting up to connect={connect_s}s; verify SPARK_CLIENT_URL "
            f"(ex.: curl -X POST {url})"
        ) from e
    except requests.exceptions.ConnectionError as e:
        raise requests.exceptions.ConnectionError(
            f"{e} | url={url!r} | verifique rede, firewall e SPARK_CLIENT_URL"
        ) from e

    parsed_ok = False
    result = None
    try:
        result = response.json()
        parsed_ok = True
        if isinstance(result, (dict, list)):
            body_repr = json.dumps(result, ensure_ascii=False, default=str)
        else:
            body_repr = repr(result)
    except ValueError:
        body_repr = response.text or ""

    max_len = 16000
    if len(body_repr) > max_len:
        body_trunc = body_repr[:max_len] + "...(truncado)"
    else:
        body_trunc = body_repr

    submit_log_full = (
        f"[Spark submit] url={url} | name={name} | "
        f"http_status={response.status_code} | body={body_trunc}"
    )
    if isinstance(result, dict):
        submit_short = (
            f"[Spark submit] http={response.status_code} | name={name} | "
            f"returncode={result.get('returncode')} | "
            f"appName={result.get('appName', '')}"
        )
    else:
        submit_short = (
            f"[Spark submit] http={response.status_code} | name={name}"
        )
    print(submit_short, flush=True)
    if logger:
        logger.info(submit_short)
        logger.debug(submit_log_full)
        for h in getattr(logger, "handlers", ()):
            try:
                h.flush()
            except Exception:
                pass
        parent = getattr(logger, "parent", None)
        if parent:
            for h in getattr(parent, "handlers", ()):
                try:
                    h.flush()
                except Exception:
                    pass

    if response_logged_event is not None:
        response_logged_event.set()

    response.raise_for_status()

    if not parsed_ok:
        raise RuntimeError(
            f"Spark response without valid JSON | http_status={response.status_code} | "
            f"text_preview={(response.text or '')[:4000]!r}"
        )

    if not isinstance(result, dict):
        return result

    if result.get("returncode") not in (None, 0):
        raise RuntimeError(f"Spark job failed: {result}")

    return result