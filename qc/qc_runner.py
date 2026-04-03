import argparse
import json
import subprocess

import numpy as np
import pandas as pd

CONFIG = {
    "min_width": 1920,
    "min_height": 1080,
    "min_fps": 30.0,
    "sync_warn_ms": 500,
    "sync_fail_ms": 1000,
    "max_delta_warn_ms": 34,
    "max_delta_fail_ms": 34,
    "max_delta_hard_fail_ms": 60,
    "max_warn_ratio": 0.02,
    "max_fail_ratio": 0.005,
    "min_session_duration_ms": 3000,
    "min_rows": 10,
    "matrix_last_row_tol": 1e-3,
    "fov_min": 1.0,
    "fov_max": 179.0,
    "allowed_fov_axis": ["horizontal", "vertical"],
    "require_activity": False,
}

REQUIRED_COLUMNS = [
    "Frame_ID",
    "Timestamp_ms",
    "FOV_Deg",
    "FOV_Axis",
    "Keyboard_Input",
    "Mouse_Delta_X",
    "Mouse_Delta_Y",
]

MATRIX_COLUMNS = [f"C2W_M{i}{j}" for i in range(4) for j in range(4)]
_SKIPPED = {"status": "PASS", "skipped": True, "issues": []}


def status_rank(status: str) -> int:
    return {"PASS": 0, "WARN": 1, "FAIL": 2}.get(status, 2)


def combine_status(*statuses):
    worst = "PASS"
    for status in statuses:
        if status_rank(status) > status_rank(worst):
            worst = status
    return worst


def to_builtin(obj):
    if isinstance(obj, dict):
        return {k: to_builtin(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_builtin(v) for v in obj]
    if isinstance(obj, tuple):
        return [to_builtin(v) for v in obj]
    if isinstance(obj, set):
        return [to_builtin(v) for v in sorted(obj)]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def parse_fraction(frac: str) -> float:
    try:
        if "/" in str(frac):
            a, b = str(frac).split("/")
            return float(a) / float(b) if float(b) != 0 else 0.0
        return float(frac)
    except Exception:
        return 0.0


def validate_schema(df: pd.DataFrame) -> dict:
    all_required = REQUIRED_COLUMNS + MATRIX_COLUMNS
    missing_columns = [col for col in all_required if col not in df.columns]

    result = {
        "status": "PASS",
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "missing_columns": missing_columns,
        "null_counts": {},
        "invalid_numeric_columns": {},
        "issues": [],
    }

    if len(df) == 0:
        result["status"] = "FAIL"
        result["issues"].append("CSV is empty")
        return result

    if len(df) < CONFIG["min_rows"]:
        result["status"] = combine_status(result["status"], "WARN")
        result["issues"].append(f"CSV has very few rows: {len(df)}")

    if missing_columns:
        result["status"] = "FAIL"
        result["issues"].append("Missing required columns")
        return result

    null_counts = {col: int(df[col].isna().sum()) for col in all_required if df[col].isna().sum() > 0}
    if null_counts:
        result["status"] = "FAIL"
        result["null_counts"] = null_counts
        result["issues"].append("Null values found in required columns")

    numeric_cols = ["Frame_ID", "Timestamp_ms", "FOV_Deg", "Mouse_Delta_X", "Mouse_Delta_Y"] + MATRIX_COLUMNS
    invalid_numeric = {
        col: int(pd.to_numeric(df[col], errors="coerce").isna().sum())
        for col in numeric_cols
        if pd.to_numeric(df[col], errors="coerce").isna().sum() > 0
    }
    if invalid_numeric:
        result["status"] = "FAIL"
        result["invalid_numeric_columns"] = invalid_numeric
        result["issues"].append("Some numeric columns contain non-numeric values")

    return result


def validate_timeline(df: pd.DataFrame) -> dict:
    result = {
        "status": "PASS",
        "frame_id_monotonic": True,
        "frame_id_sequential": True,
        "timestamp_monotonic": True,
        "negative_timestamps": 0,
        "duplicate_timestamps": 0,
        "duration_ms": None,
        "delta_ms_mean": None,
        "delta_ms_min": None,
        "delta_ms_max": None,
        "warn_intervals_count": 0,
        "fail_intervals_count": 0,
        "warn_intervals_ratio": 0.0,
        "fail_intervals_ratio": 0.0,
        "issues": [],
    }

    frame_id = pd.to_numeric(df["Frame_ID"], errors="coerce")
    ts = pd.to_numeric(df["Timestamp_ms"], errors="coerce")

    if frame_id.isna().any() or ts.isna().any():
        result["status"] = "FAIL"
        result["issues"].append("Frame_ID or Timestamp_ms contains invalid numeric data")
        return result

    frame_diff = frame_id.diff().dropna()
    ts_diff = ts.diff().dropna()

    if (frame_diff <= 0).any():
        result["frame_id_monotonic"] = False
        result["status"] = "FAIL"
        result["issues"].append("Frame_ID is not strictly increasing")

    if not (frame_diff == 1).all():
        result["frame_id_sequential"] = False
        result["status"] = "FAIL"
        result["issues"].append("Frame_ID is not sequential by 1")

    negative_ts = int((ts < 0).sum())
    result["negative_timestamps"] = negative_ts
    if negative_ts > 0:
        result["status"] = "FAIL"
        result["issues"].append("Negative timestamps found")

    if (ts_diff < 0).any():
        result["timestamp_monotonic"] = False
        result["status"] = "FAIL"
        result["issues"].append("Timestamp_ms is not monotonic increasing")

    result["duplicate_timestamps"] = int((ts_diff == 0).sum())
    duration_ms = float(ts.iloc[-1] - ts.iloc[0])
    result["duration_ms"] = duration_ms

    if duration_ms < CONFIG["min_session_duration_ms"]:
        result["status"] = combine_status(result["status"], "FAIL")
        result["issues"].append(f"Session too short: {duration_ms:.2f} ms")

    if len(ts_diff) > 0:
        total_intervals = int(len(ts_diff))
        hard_fail_intervals = int((ts_diff > CONFIG["max_delta_hard_fail_ms"]).sum())
        warn_intervals = int((ts_diff > CONFIG["max_delta_warn_ms"]).sum())
        fail_intervals = int((ts_diff > CONFIG["max_delta_fail_ms"]).sum())

        result.update({
            "delta_ms_mean": float(ts_diff.mean()),
            "delta_ms_min": float(ts_diff.min()),
            "delta_ms_max": float(ts_diff.max()),
            "warn_intervals_count": warn_intervals,
            "fail_intervals_count": fail_intervals,
            "warn_intervals_ratio": warn_intervals / total_intervals,
            "fail_intervals_ratio": fail_intervals / total_intervals,
        })

        if hard_fail_intervals > 0:
            result["status"] = combine_status(result["status"], "FAIL")
            result["issues"].append(f"Frame gap > {CONFIG['max_delta_hard_fail_ms']} ms detected ({hard_fail_intervals} intervals)")
        elif fail_intervals >= 10:
            result["status"] = combine_status(result["status"], "FAIL")
            result["issues"].append(f"Frame gaps found > {CONFIG['max_delta_fail_ms']} ms ({fail_intervals} intervals)")
        elif fail_intervals > 0:
            result["status"] = combine_status(result["status"], "WARN")
            result["issues"].append(f"Warning: frame gaps > {CONFIG['max_delta_fail_ms']} ms ({fail_intervals} intervals)")

    return result


def validate_camera_matrix(df: pd.DataFrame) -> dict:
    result = {"status": "PASS", "nan_count": 0, "inf_count": 0, "last_row_violations": 0, "issues": []}

    matrix_df = df[MATRIX_COLUMNS].apply(pd.to_numeric, errors="coerce")
    nan_count = int(matrix_df.isna().sum().sum())
    inf_count = int(np.isinf(matrix_df.to_numpy(dtype=float)).sum())

    result["nan_count"] = nan_count
    result["inf_count"] = inf_count

    if nan_count > 0:
        result["status"] = "FAIL"
        result["issues"].append("Camera matrix contains NaN")
    if inf_count > 0:
        result["status"] = "FAIL"
        result["issues"].append("Camera matrix contains Inf")

    tol = CONFIG["matrix_last_row_tol"]
    m30 = pd.to_numeric(df["C2W_M30"], errors="coerce")
    m31 = pd.to_numeric(df["C2W_M31"], errors="coerce")
    m32 = pd.to_numeric(df["C2W_M32"], errors="coerce")
    m33 = pd.to_numeric(df["C2W_M33"], errors="coerce")

    violations = int(
        ((m30.abs() > tol) | (m31.abs() > tol) | (m32.abs() > tol) | ((m33 - 1.0).abs() > tol)).sum()
    )
    result["last_row_violations"] = violations
    if violations > 0:
        result["status"] = "FAIL"
        result["issues"].append("Camera matrix last row is not close to [0,0,0,1]")

    return result


def validate_fov(df: pd.DataFrame) -> dict:
    result = {
        "status": "PASS",
        "invalid_fov_deg_rows": 0,
        "invalid_fov_axis_rows": 0,
        "issues": [],
    }

    fov_deg = pd.to_numeric(df["FOV_Deg"], errors="coerce")
    invalid_deg = int(((fov_deg < CONFIG["fov_min"]) | (fov_deg > CONFIG["fov_max"]) | fov_deg.isna()).sum())
    result["invalid_fov_deg_rows"] = invalid_deg

    axis = df["FOV_Axis"].astype(str).str.strip().str.lower()
    invalid_axis = int((~axis.isin(CONFIG["allowed_fov_axis"])).sum())
    result["invalid_fov_axis_rows"] = invalid_axis

    if invalid_deg > 0:
        result["status"] = "FAIL"
        result["issues"].append("Invalid FOV_Deg values found")

    if invalid_axis > 0:
        result["status"] = "FAIL"
        result["issues"].append("Invalid FOV_Axis values found")

    return result


def validate_input(df: pd.DataFrame) -> dict:
    result = {
        "status": "PASS",
        "mouse_dx_nonzero_count": 0,
        "mouse_dy_nonzero_count": 0,
        "keyboard_nonempty_count": 0,
        "has_activity": False,
        "issues": [],
    }

    dx = pd.to_numeric(df["Mouse_Delta_X"], errors="coerce")
    dy = pd.to_numeric(df["Mouse_Delta_Y"], errors="coerce")
    kb = df["Keyboard_Input"].fillna("").astype(str)

    if dx.isna().any() or dy.isna().any():
        result["status"] = "FAIL"
        result["issues"].append("Mouse delta contains invalid numeric data")
        return result

    kb_clean = kb.str.strip().str.lower()
    kb_nonempty = int(((kb_clean != "") & (kb_clean != "none") & (kb_clean != "0")).sum())
    dx_nonzero = int((dx != 0).sum())
    dy_nonzero = int((dy != 0).sum())

    result["mouse_dx_nonzero_count"] = dx_nonzero
    result["mouse_dy_nonzero_count"] = dy_nonzero
    result["keyboard_nonempty_count"] = kb_nonempty
    result["has_activity"] = bool(kb_nonempty > 0)

    if not kb_nonempty and not dx_nonzero and not dy_nonzero:
        result["status"] = "FAIL" if CONFIG["require_activity"] else "PASS"
        if CONFIG["require_activity"]:
            result["issues"].append("No keyboard input and no mouse movement")

    return result


def ffprobe_video(video_path: str) -> dict:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type,width,height,avg_frame_rate,r_frame_rate",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        video_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(proc.stdout)


def validate_video(video_path: str) -> dict:
    result = {
        "status": "PASS",
        "width": None,
        "height": None,
        "fps": None,
        "duration_sec": None,
        "issues": [],
    }

    try:
        meta = ffprobe_video(video_path)
    except subprocess.CalledProcessError as exc:
        result["status"] = "FAIL"
        result["issues"].append(f"ffprobe failed: {exc.stderr[:500]}")
        return result
    except Exception as exc:
        result["status"] = "FAIL"
        result["issues"].append(f"Cannot read video metadata: {str(exc)}")
        return result

    video_streams = [stream for stream in meta.get("streams", []) if stream.get("codec_type") == "video"]
    if not video_streams:
        result["status"] = "FAIL"
        result["issues"].append("No video stream found in MP4")
        return result

    stream = video_streams[0]
    width = int(stream.get("width", 0))
    height = int(stream.get("height", 0))
    fps = parse_fraction(stream.get("avg_frame_rate", "0/1"))
    duration_sec = safe_float(meta.get("format", {}).get("duration"))

    result["width"] = width
    result["height"] = height
    result["fps"] = fps
    result["duration_sec"] = duration_sec

    if width < CONFIG["min_width"] or height < CONFIG["min_height"]:
        result["status"] = "FAIL"
        result["issues"].append(
            f"Resolution below threshold: {width}x{height} < {CONFIG['min_width']}x{CONFIG['min_height']}"
        )

    if duration_sec is None or duration_sec <= 0:
        result["status"] = "FAIL"
        result["issues"].append("Invalid video duration")

    return result


def validate_sync(df: pd.DataFrame, video_result: dict) -> dict:
    result = {
        "status": "PASS",
        "csv_last_timestamp_ms": None,
        "video_duration_ms": None,
        "delta_ms": None,
        "issues": [],
    }

    if video_result["status"] == "FAIL" or not video_result.get("duration_sec"):
        result["status"] = "FAIL"
        root_video_issues = video_result.get("issues", [])
        if root_video_issues:
            result["issues"].append("Skip sync check because video failed: " + " | ".join(root_video_issues))
        else:
            result["issues"].append("Skip sync check because video metadata is invalid")
        return result

    ts = pd.to_numeric(df["Timestamp_ms"], errors="coerce")
    if ts.isna().any():
        result["status"] = "FAIL"
        result["issues"].append("Cannot validate sync because Timestamp_ms is invalid")
        return result

    csv_last_ts = float(ts.iloc[-1])
    video_duration_ms = float(video_result["duration_sec"]) * 1000.0
    delta_ms = abs(video_duration_ms - csv_last_ts)

    result["csv_last_timestamp_ms"] = csv_last_ts
    result["video_duration_ms"] = video_duration_ms
    result["delta_ms"] = delta_ms

    if delta_ms > CONFIG["sync_fail_ms"]:
        result["status"] = "FAIL"
        result["issues"].append(f"Sync drift too large: {delta_ms:.2f} ms > {CONFIG['sync_fail_ms']} ms")
    elif delta_ms > CONFIG["sync_warn_ms"]:
        result["status"] = "WARN"
        result["issues"].append(f"Sync drift warning: {delta_ms:.2f} ms > {CONFIG['sync_warn_ms']} ms")

    return result


def validate_fps_sync(video_fps: float, delta_ms_mean: float) -> dict:
    result = {"status": "PASS", "video_fps": video_fps, "csv_fps": None, "issues": []}

    if not video_fps or not delta_ms_mean or delta_ms_mean <= 0:
        result["status"] = "FAIL"
        result["issues"].append("Cannot validate FPS sync: missing data")
        return result

    csv_fps = round(1000.0 / delta_ms_mean, 2)
    result["csv_fps"] = csv_fps

    if 30 <= video_fps <= 35:
        csv_min, csv_max = 25, 35
    elif 60 <= video_fps <= 65:
        csv_min, csv_max = 55, 65
    else:
        csv_min = round(video_fps * 0.8, 2)
        csv_max = round(video_fps * 1.2, 2)

    if not (csv_min <= csv_fps <= csv_max):
        result["status"] = "FAIL"
        result["issues"].append(
            f"FPS mismatch: video={video_fps:.1f}, csv={csv_fps:.1f} (expected {csv_min}-{csv_max})"
        )

    return result


def run_qc(csv_path: str, mp4_path: str) -> dict:
    df = pd.read_csv(csv_path)
    schema = validate_schema(df)
    if schema["status"] == "FAIL":
        timeline = _SKIPPED
        matrix = _SKIPPED
        fov = _SKIPPED
        input_r = _SKIPPED
    else:
        timeline = validate_timeline(df)
        matrix = validate_camera_matrix(df)
        fov = validate_fov(df)
        input_r = validate_input(df)

    video = validate_video(mp4_path)
    if schema["status"] == "FAIL":
        sync = _SKIPPED
        fps_sync = _SKIPPED
    else:
        sync = validate_sync(df, video)
        fps_sync = validate_fps_sync(video.get("fps"), timeline.get("delta_ms_mean"))

    raw_status = combine_status(
        schema["status"],
        timeline["status"],
        matrix["status"],
        fov["status"],
        input_r["status"],
        video["status"],
        sync["status"],
        fps_sync["status"],
    )
    had_warnings = raw_status == "WARN"
    return to_builtin({
        "status": "PASS" if had_warnings else raw_status,
        "had_warnings": had_warnings,
        "files": {"csv": csv_path, "mp4": mp4_path},
        "checks": {
            "schema_validation": schema,
            "timeline_validation": timeline,
            "camera_matrix_validation": matrix,
            "fov_validation": fov,
            "input_validation": input_r,
            "video_validation": video,
            "sync_validation": sync,
            "fps_sync_validation": fps_sync,
        },
    })


def summarize_issues(report: dict) -> str:
    labels = []
    checks = report.get("checks", {})
    mapping = {
        "schema_validation": "schema",
        "timeline_validation": "timeline",
        "camera_matrix_validation": "matrix",
        "fov_validation": "fov",
        "input_validation": "input",
        "video_validation": "video",
        "sync_validation": "sync",
        "fps_sync_validation": "fps_sync",
    }
    for key, short_name in mapping.items():
        issues = checks.get(key, {}).get("issues", [])
        if issues:
            unique_issues = list(dict.fromkeys(issues))
            labels.append(f"{short_name}: " + " | ".join(unique_issues[:2]))
    return " ; ".join(labels[:4])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--mp4", required=True)
    args = parser.parse_args()

    report = run_qc(args.csv, args.mp4)
    payload = {
        "ok": True,
        "status": report.get("status", "FAIL"),
        "had_warnings": bool(report.get("had_warnings")),
        "summary": summarize_issues(report),
        "report": report,
    }
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
