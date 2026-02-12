import re
import json

from jobspy.util import create_logger

log = create_logger("Google")

# Known Google Jobs JSON keys - Google rotates these when they update their frontend.
# The heuristic fallback auto-discovers new keys at runtime.
KNOWN_JOB_KEYS = ["520084652"]


def _looks_like_job_data(arr):
    """Heuristic: check if a list matches the shape of a Google Jobs listing array.

    Google Jobs data arrays have 20+ elements with:
    - [0]: job title (non-empty string)
    - [1]: company name (string)
    - [3]: nested lists containing job URL (starts with http)
    """
    if not isinstance(arr, list) or len(arr) < 20:
        return False
    if not isinstance(arr[0], str) or len(arr[0]) < 2:
        return False
    if not isinstance(arr[1], str):
        return False
    try:
        if (
            isinstance(arr[3], list)
            and arr[3]
            and isinstance(arr[3][0], list)
            and arr[3][0]
        ):
            candidate = arr[3][0][0]
            if isinstance(candidate, str) and candidate.startswith("http"):
                return True
    except (IndexError, TypeError):
        pass
    return False


def _extract_balanced_json(text, start, max_len=50000):
    """Extract a bracket-balanced JSON array from text[start:]."""
    if start >= len(text) or text[start] != "[":
        return None
    depth = 0
    in_str = False
    esc = False
    end = min(start + max_len, len(text))
    for i in range(start, end):
        c = text[i]
        if esc:
            esc = False
            continue
        if c == "\\" and in_str:
            esc = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _remember_key(key):
    """Remember a newly discovered Google Jobs JSON key."""
    if key not in KNOWN_JOB_KEYS:
        KNOWN_JOB_KEYS.append(key)
        log.info(f"Discovered new Google Jobs data key: {key}")


def find_job_info(jobs_data):
    """Find a single job's data array in parsed JSON (pagination responses).

    Tries known keys first, then uses heuristic detection to handle
    key rotation by Google.
    """
    if isinstance(jobs_data, dict):
        # Fast path: try known keys
        for key in KNOWN_JOB_KEYS:
            if key in jobs_data and isinstance(jobs_data[key], list):
                return jobs_data[key]
        # Slow path: try all keys with heuristic validation
        for key, value in jobs_data.items():
            if isinstance(value, list) and _looks_like_job_data(value):
                _remember_key(key)
                return value
            result = find_job_info(value)
            if result:
                return result
    elif isinstance(jobs_data, list):
        if _looks_like_job_data(jobs_data):
            return jobs_data
        for item in jobs_data:
            result = find_job_info(item)
            if result:
                return result
    return None


def _extract_with_key(html_text, key):
    """Extract job arrays from HTML using a specific numeric key."""
    results = []
    marker = f'"{key}":'
    idx = 0
    while True:
        pos = html_text.find(marker, idx)
        if pos == -1:
            break
        arr_start = html_text.find("[", pos + len(marker))
        if arr_start == -1 or arr_start > pos + len(marker) + 10:
            idx = pos + len(marker)
            continue
        arr_str = _extract_balanced_json(html_text, arr_start)
        if arr_str:
            try:
                parsed = json.loads(arr_str)
                if isinstance(parsed, list):
                    results.append(parsed)
            except json.JSONDecodeError:
                pass
        idx = pos + len(marker)
    return results


def find_job_info_initial_page(html_text):
    """Extract job data arrays from the initial Google Jobs search HTML page.

    Uses bracket-balanced JSON extraction instead of fragile regex,
    and falls back to heuristic key discovery when known keys are stale.
    """
    # Pass 1: try known keys
    for key in KNOWN_JOB_KEYS:
        results = _extract_with_key(html_text, key)
        if results:
            return results

    # Pass 2: discover new numeric key via heuristic
    results = []
    for m in re.finditer(r'"(\d{6,12})":\s*\[', html_text):
        key_candidate = m.group(1)
        arr_start = m.end() - 1  # position of '['
        arr_str = _extract_balanced_json(html_text, arr_start)
        if arr_str:
            try:
                parsed = json.loads(arr_str)
                if isinstance(parsed, list) and _looks_like_job_data(parsed):
                    results.append(parsed)
                    _remember_key(key_candidate)
            except json.JSONDecodeError:
                continue
    return results
