"""
main.py — NovAtel OEM7 log analyst + documentation Q&A.

Architecture
────────────
Log file questions  → Direct Python pipeline (no agent loop):
                        1. kb_search()       — one KB call, returns raw hits
                        2. extract_params()  — one LLM call, extracts log/field/bit as JSON
                        3. run_log_tool()    — calls the right Python function directly
                        4. format_answer()   — one LLM call to format the final answer

Documentation Q&A   → LangGraph ReAct agent with kb_retriever + context_expander only
"""

from langgraph.errors import GraphRecursionError
from bedrock_agentcore import BedrockAgentCoreApp
from bedrock_agentcore.memory.client import MemoryClient
from src.model.load import load_model
from src.deterministic_mapper import get_mapper
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain.tools import tool
from langgraph.prebuilt import create_react_agent
import boto3
import base64
import os
import re
import io
import json
import time
import tempfile
import datetime
import pandas as pd
from collections import Counter, defaultdict
from botocore.config import Config as BotocoreConfig
from urllib.parse import urlparse

# ── GPS helpers ───────────────────────────────────────────────────────
_GPS_EPOCH    = datetime.datetime(1980, 1, 6, tzinfo=datetime.timezone.utc)
_LEAP_SECONDS = 18

def gps_to_utc(week: int, seconds: float) -> datetime.datetime:
    return _GPS_EPOCH + datetime.timedelta(seconds=week * 604800 + seconds - _LEAP_SECONDS)

def gps_to_utc_str(week: int, seconds: float) -> str:
    if week <= 0:
        return ""
    return gps_to_utc(week, seconds).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# ── Config ────────────────────────────────────────────────────────────
REGION          = "us-east-1"
MEMORY_ID       = os.getenv("MEMORY_ID")
S3_BUCKET       = os.getenv("S3_BUCKET")
KB_ID           = os.getenv("KB_ID", "FH00WKSBPL")
GUARDRAIL_ID    = os.getenv("GUARDRAIL_ID", "")
GUARDRAIL_VER   = os.getenv("GUARDRAIL_VERSION", "1")
ACTOR_ID        = "default-user"
SIZE_THRESHOLD  = 5 * 1024 * 1024
MAX_RESULTS     = int(os.getenv("MAX_RESULTS", "15"))
EXPANSION_PAGES = int(os.getenv("EXPANSION_PAGES", "2"))
_LLM_COLS       = {"element_id", "element_type", "content_markdown", "page_number"}

app = BedrockAgentCoreApp()

# ── Status tracking for UI updates ───────────────────────────────────
_current_status: dict[str, str] = {}

def set_status(session_id: str, status: str):
    """Set current processing status for UI display."""
    _current_status[session_id] = status
    print(f"[STATUS] {session_id}: {status}")

def get_status(session_id: str) -> str:
    """Get current processing status."""
    return _current_status.get(session_id, "")

def clear_status(session_id: str):
    """Clear status after completion."""
    _current_status.pop(session_id, None)

# ── Lazy singletons ───────────────────────────────────────────────────
_llm = _memory_client = _s3_client = _kb_client = _bedrock_runtime = None
_BOTO_CONFIG = BotocoreConfig(read_timeout=300)
_gnss_kb = None

def get_llm():
    global _llm
    if _llm is None:
        _llm = load_model()
    return _llm

def get_gnss_kb():
    """Load GNSS knowledge base (79KB markdown) for LLM context."""
    global _gnss_kb
    if _gnss_kb is None:
        try:
            kb_path = "gnss_knowledge_base.md"
            with open(kb_path, 'r', encoding='utf-8') as f:
                _gnss_kb = f.read()
            print(f"[KB] Loaded GNSS knowledge base: {len(_gnss_kb)} chars")
        except FileNotFoundError:
            print(f"[KB] Warning: gnss_knowledge_base.md not found, using empty KB")
            _gnss_kb = ""
        except Exception as e:
            print(f"[KB] Error loading knowledge base: {e}")
            _gnss_kb = ""
    return _gnss_kb

def get_memory_client():
    global _memory_client
    if _memory_client is None:
        _memory_client = MemoryClient(region_name=REGION)
    return _memory_client

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name="ap-south-1", config=_BOTO_CONFIG)
    return _s3_client

def get_kb_client():
    global _kb_client
    if _kb_client is None:
        _kb_client = boto3.client("bedrock-agent-runtime", region_name="us-west-2", config=_BOTO_CONFIG)
    return _kb_client

def get_bedrock_runtime():
    global _bedrock_runtime
    if _bedrock_runtime is None:
        _bedrock_runtime = boto3.client("bedrock-runtime", region_name=REGION, config=_BOTO_CONFIG)
    return _bedrock_runtime

# ── Guardrail ─────────────────────────────────────────────────────────
def apply_guardrail(text: str, source: str = "INPUT") -> str:
    if not GUARDRAIL_ID:
        return text
    try:
        resp = get_bedrock_runtime().apply_guardrail(
            guardrailIdentifier=GUARDRAIL_ID,
            guardrailVersion=GUARDRAIL_VER,
            source=source,
            content=[{"text": {"text": text}}],
        )
        if resp["action"] == "GUARDRAIL_INTERVENED":
            blocked = resp.get("outputs", [{}])[0].get("text", "Content blocked by guardrail.")
            raise ValueError(blocked)
    except ValueError:
        raise
    except Exception as e:
        print(f"[GUARDRAIL] error: {e}")
    return text

# ── KB helpers ────────────────────────────────────────────────────────
_tool_call_log: list[dict] = []
_csv_cache: dict[str, pd.DataFrame] = {}

def _download_and_parse_key(bucket: str, key: str) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tmp:
        get_s3_client().download_fileobj(bucket, key, tmp)
        tmp.flush()
        tmp_path = tmp.name
    try:
        with open(tmp_path, "r", encoding="utf-8") as f:
            raw = f.read()
        try:
            data = json.loads(raw)
            rows = [e["contentMetadata"] for e in data.get("fileContents", []) if "contentMetadata" in e]
            df = pd.DataFrame(rows)
        except (json.JSONDecodeError, KeyError):
            df = pd.read_csv(io.StringIO(raw))
    finally:
        os.unlink(tmp_path)
    return df

def _download_and_parse(source_uri: str) -> pd.DataFrame:
    if source_uri in _csv_cache:
        return _csv_cache[source_uri]
    parsed = urlparse(source_uri)
    bucket = parsed.netloc
    key    = parsed.path.lstrip("/")
    df = _download_and_parse_key(bucket, key)
    if "element_id" not in df.columns and not key.startswith("Output/"):
        df = _download_and_parse_key(bucket, f"Output/{key}")
    if "page_number" in df.columns:
        df["page_number"] = pd.to_numeric(df["page_number"], errors="coerce").fillna(0).astype(int)
    _csv_cache[source_uri] = df
    return df

def _resolve_data_uri(source_uri: str) -> str:
    if not source_uri.lower().endswith(".pdf"):
        return source_uri
    for entry in reversed(_tool_call_log):
        if entry.get("tool") != "kb_retriever":
            continue
        for el in entry["result"].get("elements", []):
            if el.get("source_uri") == source_uri and el.get("csv_source_uri"):
                return el["csv_source_uri"]
    raise ValueError(f"Cannot resolve data URI from PDF path: {source_uri}")

# ── NovAtel ASCII log parser ──────────────────────────────────────────
_log_store: dict[str, dict] = {}

_ASCII_FULL_RE = re.compile(
    r"^#(?P<log_name>[A-Z0-9_]+),"
    r"(?P<header>[^;]*);"
    r"(?P<fields>.*?)"
    r"(?:\*[0-9a-fA-F]{1,8})?\s*$"
)

def _parse_line(line: str) -> dict | None:
    m = _ASCII_FULL_RE.match(line.strip())
    if not m:
        return None
    g = m.groupdict()
    h = [p.strip() for p in g["header"].split(",")]

    def _get(i, d=""): return h[i] if i < len(h) else d
    def _tryf(v, d=0.0):
        try: return float(v)
        except: return d
    def _tryi(v, d=0):
        try: return int(v)
        except: return d

    log_name   = g["log_name"]
    normalized = log_name[:-1] if (log_name.endswith("A") and len(log_name) > 4) else log_name
    week       = _tryi(_get(4))
    seconds    = _tryf(_get(5))

    return {
        "log_name":     normalized,
        "log_name_raw": log_name,
        "port":         _get(0),
        "seq":          _tryi(_get(1)),
        "idle_pct":     _tryf(_get(2)),
        "time_status":  _get(3),
        "week":         week,
        "seconds":      seconds,
        "utc_time":     gps_to_utc_str(week, seconds),
        "rx_status":    _get(6),   # receiver status word in every message header
        "fields_raw":   g["fields"],
    }

def parse_novatel_ascii(text: str) -> pd.DataFrame:
    records, skipped = [], 0
    for line in text.splitlines():
        if not line.strip():
            continue
        rec = _parse_line(line)
        if rec:
            records.append(rec)
        else:
            skipped += 1
    print(f"[PARSE] matched={len(records)} skipped={skipped}")
    return pd.DataFrame(records) if records else pd.DataFrame()

def _summarize_log(df: pd.DataFrame, filename: str) -> str:
    if df.empty:
        return f"Uploaded file '{filename}' had no parseable NovAtel ASCII log lines."
    log_counts = Counter(df["log_name_raw"])
    top_logs   = ", ".join(f"{n}({c})" for n, c in log_counts.most_common(10))
    VALID_TIME = {"FINESTEERING","FINE","FINEBACKUPSTEERING","FINEADJUSTING",
                  "COARSE","COARSESTEERING","COARSEADJUSTING","FREEWHEELING"}
    valid = df[df["time_status"].isin(VALID_TIME) & (df["week"] > 0)]
    parts = [f"Log '{filename}': {len(df)} records, {len(log_counts)} distinct log types.",
             f"Top log types: {top_logs}."]
    if not valid.empty:
        weeks   = sorted(valid["week"].unique().tolist())
        t_start = valid["seconds"].min()
        t_end   = valid["seconds"].max()
        w_start = int(valid.loc[valid["seconds"].idxmin(), "week"])
        w_end   = int(valid.loc[valid["seconds"].idxmax(), "week"])
        dur     = (weeks[-1]-weeks[0])*604800+(t_end-t_start) if len(weeks)>1 else t_end-t_start
        parts.append(
            f"File time range: {gps_to_utc_str(w_start,t_start)} to "
            f"{gps_to_utc_str(w_end,t_end)} (duration {dur:.1f}s = {dur/60:.2f} min)."
        )
    return " ".join(parts)

def ingest_log_file(file_bytes: bytes, filename: str, session_id: str) -> dict:
    t0   = time.time()
    text = file_bytes.decode("utf-8", errors="replace")
    df   = parse_novatel_ascii(text)
    _log_store[session_id] = {"df": df, "summary": _summarize_log(df, filename), "filename": filename}
    
    # Clear docs cache on new file upload to free memory
    _docs_cache.clear()
    
    print(f"[INGEST] {filename} parsed {len(df)} records session={session_id} took={time.time()-t0:.2f}s")
    return {"filename": filename, "records": len(df),
            "log_types": int(df["log_name"].nunique()) if not df.empty else 0,
            "summary": _log_store[session_id]["summary"]}

# ── Per-request session context ───────────────────────────────────────
_current_session: dict[str, str] = {"id": ""}

# ── Field index conversion ────────────────────────────────────────────
def _doc_field_to_body_index(field_index: int) -> int:
    """
    NovAtel docs: fields are 1-based, header = field 1, first body field = field 2.
    Our fields_raw list is 0-based starting from body field 2.
    So: body_index = doc_field_index - 2.
    If already 0-based (field_index < 2), pass through unchanged.
    """
    return (field_index - 2) if field_index >= 2 else field_index

# ── Core analysis functions (plain Python, no agent) ──────────────────

def _safe_parse_hex(fv: str) -> int | None:
    fv = fv.strip()
    try:
        return int(fv, 16)
    except (ValueError, TypeError):
        try:
            return int(fv)
        except (ValueError, TypeError):
            return None

def do_check_bit(session_id: str, log_name: str, field_index: int,
                 bit_position: int, time_from: float = None,
                 time_to: float = None, max_results: int = 1000) -> dict:
    """
    Check which records of log_name have a specific bit set in a specific field.
    field_index is the NovAtel doc field number (1-based, header = field 1).
    """
    entry = _log_store.get(session_id)
    if not entry:
        return {"status": "error", "error": "No log file uploaded."}
    df = entry["df"]

    if log_name.upper().endswith("A") and len(log_name) > 4:
        log_name = log_name[:-1]

    filtered = df[df["log_name"].str.upper() == log_name.upper()]
    if time_from is not None:
        filtered = filtered[filtered["seconds"] >= time_from]
    if time_to is not None:
        filtered = filtered[filtered["seconds"] <= time_to]

    if filtered.empty:
        return {"status": "success", "log_name": log_name, "total_checked": 0,
                "matches_found": 0, "records_with_bit_set": [],
                "note": f"No {log_name} records found in the file."}

    body_index = _doc_field_to_body_index(field_index)
    print(f"[CHECK_BIT] log={log_name} doc_field={field_index} body_index={body_index} bit={bit_position} mask={hex(1 << bit_position)}")

    # Sample first record to confirm we're reading the right field
    sample_row = filtered.iloc[0]
    sample_fields = [f.strip() for f in sample_row.get("fields_raw", "").split(",")]
    print(f"[CHECK_BIT] first record has {len(sample_fields)} body fields")
    print(f"[CHECK_BIT] field[{body_index}] = '{sample_fields[body_index] if body_index < len(sample_fields) else 'OUT OF RANGE'}'")

    mask    = 1 << bit_position
    matches = []
    errors  = 0
    checked = 0

    for _, row in filtered.iterrows():
        fields = [f.strip() for f in row.get("fields_raw", "").split(",")]
        if body_index >= len(fields):
            errors += 1
            continue
        val = _safe_parse_hex(fields[body_index])
        if val is None:
            errors += 1
            continue
        checked += 1
        if val & mask:
            if len(matches) < max_results:
                matches.append({
                    "utc_time":        row.get("utc_time", ""),
                    "gps_week":        int(row["week"]),
                    "gps_seconds":     float(row["seconds"]),
                    "field_value_hex": hex(val),
                })

    print(f"[CHECK_BIT] total={len(filtered)} checked={checked} matches={len(matches)} errors={errors}")
    return {
        "status":           "success",
        "log_name":         log_name,
        "doc_field_index":  field_index,
        "body_index_used":  body_index,
        "bit_position":     bit_position,
        "bit_mask_hex":     hex(mask),
        "total_checked":    checked,
        "matches_found":    len(matches),
        "parse_errors":     errors,
        "records_with_bit_set": matches,
    }

def do_analyze_field(session_id: str, log_name: str, field_index: int) -> dict:
    """Compute min/max/avg for a numeric field. field_index is the NovAtel doc field number."""
    entry = _log_store.get(session_id)
    if not entry:
        return {"status": "error", "error": "No log file uploaded."}
    df = entry["df"]

    if log_name.upper().endswith("A") and len(log_name) > 4:
        log_name = log_name[:-1]

    filtered = df[df["log_name"].str.upper() == log_name.upper()]
    if filtered.empty:
        return {"status": "error", "error": f"No {log_name} records found."}

    body_index = _doc_field_to_body_index(field_index)
    print(f"[ANALYZE] log={log_name} doc_field={field_index} body_index={body_index}")

    values, recs = [], []
    for _, row in filtered.iterrows():
        fields = [f.strip() for f in row.get("fields_raw", "").split(",")]
        if body_index >= len(fields):
            continue
        try:
            val = float(fields[body_index])
            values.append(val)
            recs.append({"value": val, "utc_time": row.get("utc_time",""),
                         "seconds": row["seconds"], "week": int(row["week"])})
        except (ValueError, TypeError):
            continue

    if not values:
        return {"status": "error", "error": f"No numeric values at field {field_index} of {log_name}."}

    min_val = min(values)
    max_val = max(values)
    return {
        "status": "success", "log_name": log_name,
        "doc_field_index": field_index, "body_index_used": body_index,
        "total_records": len(filtered), "valid_values": len(values),
        "min_value": min_val, "max_value": max_val,
        "average_value": sum(values) / len(values),
        "range": max_val - min_val,
        "min_occurred_at": next(r for r in recs if r["value"] == min_val),
        "max_occurred_at": next(r for r in recs if r["value"] == max_val),
    }

def do_summarize_log(session_id: str, log_name: str,
                     question: str, limit: int = None) -> dict:
    """
    Return records from a log and let the LLM summarize them in context of the question.
    Used for status/detection logs where raw field arrays need interpretation.
    Fully generic - works for any log type.
    If limit is None, processes ALL records (no sampling).
    """
    entry = _log_store.get(session_id)
    if not entry:
        return {"status": "error", "error": "No log file uploaded."}
    df = entry["df"]

    norm = log_name[:-1] if (log_name.upper().endswith("A") and len(log_name) > 4) else log_name
    filtered = df[df["log_name"].str.upper() == norm.upper()]
    if filtered.empty:
        return {"status": "error", "error": f"No {log_name} records found in the file."}

    total = len(filtered)
    
    # If limit is specified and total exceeds it, sample evenly
    # Otherwise process ALL records
    if limit and total > limit:
        step = total // limit
        sample = filtered.iloc[::step].head(limit)
    else:
        sample = filtered
    
    records = []
    for _, row in sample.iterrows():
        fields_raw = row.get("fields_raw", "")
        # Parse all fields (no truncation for deterministic analysis)
        fields_parsed = [f.strip() for f in fields_raw.split(",")]
        
        records.append({
            "utc_time":      row.get("utc_time", ""),
            "week":          int(row["week"]),
            "seconds":       float(row["seconds"]),
            "rx_status":     row.get("rx_status", ""),
            "fields_parsed": fields_parsed,
        })

    return {
        "status":        "success",
        "log_name":      log_name,
        "total_records": total,
        "sample_size":   len(records),
        "records":       records,
        "question":      question,
    }

# ── Subject extractor ────────────────────────────────────────────────
_SUBJECT_PROMPT = """Extract the core technical subject from the user's question. 
Return ONLY 1-3 words that name the phenomenon, measurement, or event being asked about.
No sentences. No explanation. Just the subject words.

Examples:
  "do we have spoofing in this file"  → spoofing detection
  "identify interference events"       → interference detection
  "what is the maximum height"         → height maximum
  "show me jamming records"            → jamming detection
  "any position errors"                → position error
  "when did spoofing occur"            → spoofing detection
  "check for jamming"                  → jamming detection
  "antenna status changes"             → antenna status
  "tracking issues"                    → tracking status
  "signal quality problems"            → signal quality

Question: {question}"""

def extract_subject(question: str) -> str:
    """Extract the core subject from a user question - fully generic."""
    try:
        response = get_llm().invoke([HumanMessage(
            content=_SUBJECT_PROMPT.format(question=question)
        )])
        subject = response.content.strip().lower()
        subject = subject.split("\n")[0].strip("\"'.,")
        print(f"[SUBJECT] '{question}' → '{subject}'")
        return subject
    except Exception as e:
        print(f"[SUBJECT] fallback: {e}")
        # Generic fallback - extract meaningful words
        filler = {"identify","find","show","list","check","detect","any","all",
                  "in","this","file","the","is","are","there","do","we","have",
                  "me","a","an","of","for","from","what","how","when","records",
                  "did","was","were","does","can","could","would","should"}
        words  = [w for w in question.lower().split() if w not in filler]
        return " ".join(words[:3]) or question

# ── KB search (pure Python, no agent) ────────────────────────────────
def kb_search(query: str, max_results: int = MAX_RESULTS) -> list[dict]:
    t0 = time.time()
    try:
        response = get_kb_client().retrieve(
            knowledgeBaseId=KB_ID,
            retrievalQuery={"text": query},
            retrievalConfiguration={"vectorSearchConfiguration": {"numberOfResults": max_results}},
        )
        elements = []
        for result in response.get("retrievalResults", []):
            content  = result.get("content", {}).get("text", "")
            metadata = result.get("metadata", {})
            elements.append({
                "element_id":       metadata.get("element_id", ""),
                "content_markdown": content,
                "page_number":      int(metadata.get("page_number", 0)),
                "score":            result.get("score", 0.0),
                "source_uri":       metadata.get("x-amz-bedrock-kb-source-uri", ""),
                "csv_source_uri":   metadata.get("csv_source_uri", ""),
            })
        print(f"[KB] query='{query}' results={len(elements)} took={time.time()-t0:.2f}s")
        return elements
    except Exception as e:
        print(f"[KB] error: {e}")
        return []


# ── NovAtel live docs fetcher ─────────────────────────────────────────
import urllib.request
from html.parser import HTMLParser

class _TableTextExtractor(HTMLParser):
    """Extract plain text from HTML, preserving table row structure."""
    def __init__(self):
        super().__init__()
        self.rows: list[str] = []
        self._cell_texts: list[str] = []
        self._current: list[str] = []
        self._in_cell = False
        self._skip_tags = {"script", "style", "nav", "header", "footer"}
        self._skip = False
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._skip_tags:
            self._skip = True
            self._skip_depth = 0
        if self._skip:
            self._skip_depth += 1
            return
        if tag in ("td", "th"):
            self._in_cell = True
            self._current = []

    def handle_endtag(self, tag):
        if self._skip:
            self._skip_depth -= 1
            if self._skip_depth <= 0:
                self._skip = False
            return
        if tag in ("td", "th"):
            self._in_cell = False
            self._cell_texts.append(" ".join(self._current).strip())
        if tag == "tr":
            if self._cell_texts:
                self.rows.append(" | ".join(self._cell_texts))
            self._cell_texts = []

    def handle_data(self, data):
        if self._skip or not self._in_cell:
            return
        text = data.strip()
        if text:
            self._current.append(text)

# Cache for live docs (session-scoped, cleared on new file upload)
_docs_cache: dict[str, str] = {}

def fetch_novatel_log_docs(log_name: str) -> str:
    """
    Fetch the live NovAtel OEM7 documentation page for a log and return
    the field/bit table rows as plain text. Works for any log name.
    Returns empty string on failure (network unavailable etc).
    Cached per session to avoid repeated web requests.
    """
    # Check cache first
    if log_name in _docs_cache:
        print(f"[DOCS] Using cached docs for {log_name}")
        return _docs_cache[log_name]
    
    # Strip trailing A (RXSTATUSA → RXSTATUS) for the URL
    url_name = log_name[:-1] if (log_name.upper().endswith("A") and len(log_name) > 4) else log_name
    url = f"https://docs.novatel.com/OEM7/Content/Logs/{url_name}.htm"
    t0  = time.time()
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 NovAtelAgent/1.0"}
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        parser = _TableTextExtractor()
        parser.feed(html)

        # Keep only rows that look like field/bit table rows (contain numbers and descriptions)
        useful = [r for r in parser.rows if r.strip() and len(r) > 10]
        result = "\n".join(useful)
        
        # Cache the result
        _docs_cache[log_name] = result
        
        print(f"[DOCS] Fetched {url_name} docs: {len(useful)} table rows took={time.time()-t0:.2f}s")
        return result
    except Exception as e:
        print(f"[DOCS] Could not fetch {url}: {e}")
        return ""

# ── Param extraction (one LLM call → structured JSON) ─────────────────
_EXTRACT_PROMPT = """You are a NovAtel OEM7 documentation parser with expertise in all receiver logs.

You are given the OFFICIAL NovAtel documentation table for the log, plus supplementary KB excerpts.
Extract the exact field and bit that answers the user's question.
Output ONLY a single JSON object. No explanation. No markdown. No extra text.

JSON fields:
  log_name      — use exactly: {log_name}
  field_index   — the field NUMBER from the left column of the NovAtel field table (integer, 1-based, header=field 1)
  bit_position  — bit number (0=LSB) from the bit table. Use null if not a flag/event question.
  question_type — classify the question as exactly one of:
    "bit_check"   : looking for whether a specific bit is set (yes/no detection, e.g. jamming detected, spoofing active, antenna status)
    "numeric_stat": asking for min/max/average/range of a MEASURED physical value (e.g. height in metres, speed in m/s, temperature)
    "raw_listing" : everything else — showing records, identifying events from a status/detection log, listing what occurred

CRITICAL RULES FOR ACCURACY:
1. ALWAYS prefer the OFFICIAL DOCS over KB excerpts — the docs are the authoritative source.
2. For detection/status questions:
   - Look for rows with "Detected", "Detection Status", "Status", or the exact phenomenon name in the Description column
   - IGNORE rows with "Calibration", "Required", "Priority Mask", "Set Mask", "Clear Mask", "Reserved"
   - Choose the row that directly answers the user's question
3. field_index is the exact integer in the leftmost column of the field table (1-based).
4. For bit questions, ALWAYS provide bit_position as an integer, never null.
5. Read the ENTIRE documentation table carefully before choosing - don't pick the first match.
6. Respond with ONLY valid JSON. No explanation, no markdown fences.

Log: {log_name}
Question: {question}

OFFICIAL NOVATEL DOCUMENTATION (field and bit tables for {log_name}):
{official_docs}

SUPPLEMENTARY KB EXCERPTS:
{kb_content}"""


def extract_log_params(question: str, kb_elements: list[dict],
                       log_name: str = None, official_docs: str = "",
                       top_n: int = 8) -> dict | None:
    """
    One LLM call to extract log/field/bit params.
    Uses official NovAtel docs (fetched live) as primary source,
    KB excerpts as supplementary context.
    Fully generic - works for ANY log type and ANY question.
    """
    for i, el in enumerate(kb_elements[:top_n]):
        print(f"[KB_HIT] #{i+1} score={el['score']:.3f} preview={el['content_markdown'][:120].replace(chr(10),' ')}")

    kb_content = "\n\n---\n\n".join(
        f"[Score: {el['score']:.3f}]\n{el['content_markdown']}"
        for el in kb_elements[:top_n]
    ) if kb_elements else "No KB results."

    # Use more focused docs - just field tables, not verbose descriptions
    # Look for table-like content (lines with | separators)
    if official_docs:
        doc_lines = official_docs.split('\n')
        table_lines = [line for line in doc_lines if '|' in line and len(line) > 20]
        docs_excerpt = '\n'.join(table_lines[:150])  # First 150 table rows
        if not docs_excerpt:
            docs_excerpt = official_docs[:4000]  # Fallback to first 4000 chars
    else:
        docs_excerpt = "Not available — rely on KB excerpts."

    prompt = _EXTRACT_PROMPT.format(
        log_name=log_name or "unknown",
        question=question,
        official_docs=docs_excerpt,
        kb_content=kb_content,
    )

    response = None
    try:
        response = get_llm().invoke([HumanMessage(content=prompt)])
        raw = response.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        params = json.loads(raw)
        if log_name and not params.get("log_name"):
            params["log_name"] = log_name
        
        print(f"[EXTRACT] ✓ log={params.get('log_name')} field={params.get('field_index')} "
              f"bit={params.get('bit_position')} type={params.get('question_type')}")
        return params
    except Exception as e:
        raw_out = response.content[:300] if response else "no response"
        print(f"[EXTRACT] ✗ failed: {e} — raw='{raw_out}'")
        return None



# ── FAST DETERMINISTIC FORMATTER (NO LLM) ────────────────────────────
def format_answer_deterministic(question: str, tool_result: dict, intent: dict) -> str:
    """
    Ultra-fast answer formatting WITHOUT LLM for deterministic queries.
    Uses templates based on analysis type.
    Target: < 100ms
    """
    t0 = time.time()
    analysis_type = intent.get('analysis_type')
    log_name = tool_result.get('log_name', intent.get('log_type', 'unknown'))
    
    # Bit check formatting (jamming, spoofing, antenna, etc.)
    if analysis_type == 'bit_check':
        matches = tool_result.get('matches_found', 0)
        total = tool_result.get('total_checked', 0)
        bit_pos = tool_result.get('bit_position', 0)
        field_idx = tool_result.get('doc_field_index', 0)
        
        if matches == 0:
            result = f"✅ **No {intent.get('description', 'events')} detected.**\n\n"
            result += f"Checked {total:,} {log_name} records — bit {bit_pos} in field {field_idx} was never set.\n\n"
            result += "The receiver did not flag this condition during the observation period."
        else:
            records = tool_result.get('records_with_bit_set', [])
            result = f"⚠️ **{intent.get('description', 'Events')} detected in {matches:,} of {total:,} records.**\n\n"
            result += f"**Log:** {log_name}\n"
            result += f"**Field:** {field_idx}, **Bit:** {bit_pos}\n\n"
            
            if matches <= 10:
                result += "**All occurrences:**\n\n"
                for rec in records[:10]:
                    result += f"- {rec.get('utc_time', 'N/A')} (GPS Week {rec.get('gps_week', 0)}, {rec.get('gps_seconds', 0):.1f}s)\n"
            else:
                result += f"**Sample occurrences (showing first 10 of {matches}):**\n\n"
                for rec in records[:10]:
                    result += f"- {rec.get('utc_time', 'N/A')}\n"
                result += f"\n*...and {matches - 10} more occurrences*\n"
        
        print(f"[FORMAT_FAST] bit_check took={time.time()-t0:.3f}s")
        return result
    
    # Numeric stat formatting (height, velocity, signal quality, etc.)
    elif analysis_type == 'numeric_stat':
        min_val = tool_result.get('min_value', 0)
        max_val = tool_result.get('max_value', 0)
        avg_val = tool_result.get('average_value', 0)
        range_val = tool_result.get('range', 0)
        total = tool_result.get('total_records', 0)
        valid = tool_result.get('valid_values', 0)
        field_idx = tool_result.get('doc_field_index', 0)
        unit = intent.get('unit', '')
        
        min_time = tool_result.get('min_occurred_at', {}).get('utc_time', 'N/A')
        max_time = tool_result.get('max_occurred_at', {}).get('utc_time', 'N/A')
        
        result = f"**{intent.get('description', 'Statistics')}**\n\n"
        result += f"**Log:** {log_name}, **Field:** {field_idx}\n\n"
        result += "| Metric | Value | Timestamp (UTC) |\n"
        result += "|--------|-------|----------------|\n"
        result += f"| **Minimum** | {min_val:.3f} {unit} | {min_time} |\n"
        result += f"| **Maximum** | {max_val:.3f} {unit} | {max_time} |\n"
        result += f"| **Average** | {avg_val:.3f} {unit} | - |\n"
        result += f"| **Range** | {range_val:.3f} {unit} | - |\n\n"
        result += f"*Analyzed {valid:,} valid values from {total:,} total {log_name} records.*"
        
        print(f"[FORMAT_FAST] numeric_stat took={time.time()-t0:.3f}s")
        return result
    
    # Raw listing - need LLM for complex interpretation
    else:
        print(f"[FORMAT_FAST] falling back to LLM for raw_listing")
        return None  # Signal to use LLM path

# ── FAST INTERFERENCE FORMATTER (NO LLM) ──────────────────────────────
def format_interference_analysis(question: str, tool_result: dict, intent: dict) -> str:
    """
    Ultra-fast interference analysis formatting WITHOUT LLM.
    Parses ITDETECTSTATUS records and formats detailed interference report.
    Target: < 500ms
    """
    t0 = time.time()
    
    log_name = tool_result.get('log_name', 'ITDETECTSTATUS')
    total_records = tool_result.get('total_records', 0)
    sample_size = tool_result.get('sample_size', 0)
    records = tool_result.get('records', [])
    
    if total_records == 0:
        return f"✅ **No interference detected.**\n\nNo {log_name} records found in this file."
    
    # Parse interference data from sampled records
    interference_count = 0
    rf_paths = {}
    detection_types = {}
    freq_min, freq_max = float('inf'), float('-inf')
    power_min, power_max = float('inf'), float('-inf')
    timestamps = []
    
    for rec in records:
        fields = rec.get('fields_parsed', [])
        utc_time = rec.get('utc_time', 'N/A')
        
        # ITDETECTSTATUS structure: field 0 = noOfEntries, then repeating groups
        # Each interference entry has: rfPath, detectionType, centerFreq, bandwidth, power, etc.
        if len(fields) > 0:
            try:
                num_entries = int(fields[0]) if fields[0] else 0
                if num_entries > 0:
                    interference_count += 1
                    timestamps.append(utc_time)
                    
                    # Parse first interference entry (fields 1-9)
                    if len(fields) >= 5:
                        rf_path = fields[1] if len(fields) > 1 else 'unknown'
                        detection_type = fields[2] if len(fields) > 2 else 'unknown'
                        center_freq = float(fields[3]) if len(fields) > 3 and fields[3] else 0
                        power = float(fields[5]) if len(fields) > 5 and fields[5] else 0
                        
                        rf_paths[rf_path] = rf_paths.get(rf_path, 0) + 1
                        detection_types[detection_type] = detection_types.get(detection_type, 0) + 1
                        
                        if center_freq > 0:
                            freq_min = min(freq_min, center_freq)
                            freq_max = max(freq_max, center_freq)
                        
                        if power != 0:
                            power_min = min(power_min, power)
                            power_max = max(power_max, power)
            except (ValueError, IndexError):
                pass
    
    # Format the report
    if interference_count == 0:
        result = f"✅ **No interference detected in this file.**\n\n"
        result += f"Analyzed all {total_records:,} {log_name} records — "
        result += "no interference entries were found."
        print(f"[FORMAT_INTERFERENCE] took={time.time()-t0:.3f}s")
        return result
    
    # Calculate percentage
    pct = (interference_count / sample_size * 100) if sample_size > 0 else 0
    
    # Determine if this is full analysis or sampled
    is_full_analysis = (sample_size == total_records)
    
    if is_full_analysis:
        result = f"⚠️ **Interference detected throughout this file — {interference_count:,} of {total_records:,} records ({pct:.0f}%) contain active interference.**\n\n"
    else:
        result = f"⚠️ **Interference detected — {interference_count:,} of {sample_size:,} sampled records ({pct:.0f}%) contain active interference.**\n\n"
    
    # Time range
    if timestamps:
        first_time = timestamps[0]
        last_time = timestamps[-1]
        if first_time == last_time:
            result += f"**Observation time:** {first_time}\n\n"
        else:
            result += f"**Time range:** {first_time} to {last_time}\n\n"
    
    # Interference characteristics
    result += "**Interference Characteristics:**\n\n"
    
    # RF paths affected
    if rf_paths:
        result += "**RF Paths Affected:**\n"
        for path, count in sorted(rf_paths.items(), key=lambda x: -x[1]):
            result += f"- {path}: {count} occurrences\n"
        result += "\n"
    
    # Detection methods
    if detection_types:
        result += "**Detection Methods:**\n"
        for dtype, count in sorted(detection_types.items(), key=lambda x: -x[1]):
            result += f"- {dtype}: {count} occurrences\n"
        result += "\n"
    
    # Frequency range
    if freq_min != float('inf') and freq_max != float('-inf'):
        result += f"**Frequency Range:** {freq_min:.3f} MHz to {freq_max:.3f} MHz\n"
        
        # Check if it's targeting GPS L1
        if 1575.0 < freq_min < 1576.0 and 1575.0 < freq_max < 1576.0:
            result += f"- **Target:** GPS L1 band (~1575.42 MHz) — narrowband interference\n"
        result += "\n"
    
    # Power range
    if power_min != float('inf') and power_max != float('-inf'):
        result += f"**Signal Power Range:** {power_min:.1f} to {power_max:.1f} dBm\n\n"
    
    # Conclusion
    result += "**Assessment:**\n"
    if interference_count == sample_size:
        result += "The interference is **persistent and continuous** — present in every sampled record. "
    elif pct > 75:
        result += "The interference is **frequent** — present in most sampled records. "
    elif pct > 25:
        result += "The interference is **intermittent** — present in some sampled records. "
    else:
        result += "The interference is **sporadic** — present in a few sampled records. "
    
    # Check for GPS L1 targeting
    if freq_min != float('inf') and 1575.0 < freq_min < 1576.0:
        result += "The interference is **targeting the GPS L1 frequency** (~1575.42 MHz), which is consistent with a narrowband jammer or CW interferer. "
    
    if is_full_analysis:
        result += f"\n\n*Full file analysis: all {total_records:,} {log_name} records processed.*"
    else:
        result += f"\n\n*Analysis based on {sample_size:,} sampled records from {total_records:,} total {log_name} records.*"
    
    print(f"[FORMAT_INTERFERENCE] took={time.time()-t0:.3f}s")
    return result

# ── FAST TRACKSTAT C/No FORMATTER (NO LLM) ────────────────────────────
def format_trackstat_cno(question: str, tool_result: dict, intent: dict, handler: str) -> str:
    """
    Ultra-fast TRACKSTAT C/No analysis formatting WITHOUT LLM.
    Parses TRACKSTAT records to extract C/No from all satellites.
    
    TRACKSTAT structure (from logparsers.js):
    - Header: solutionStatus(0), positionType(4), cutoff(8), numberOfChannels(12)
    - Per satellite (40-byte records): PRN(16), glonassFrequency(18), 
      channelTrackingStatus(20), pseudoRange(24), doppler(32), 
      carrierNoiseRatio(36), lockTime(40), pseudoRangeResidual(44), 
      reject(48), pseudoRangeWeight(52)
    
    Target: < 500ms
    """
    t0 = time.time()
    
    log_name = tool_result.get('log_name', 'TRACKSTAT')
    total_records = tool_result.get('total_records', 0)
    records = tool_result.get('records', [])
    
    if total_records == 0:
        return f"✅ **No {log_name} records found in this file.**"
    
    # Parse C/No values from all satellites in all records
    all_cno_values = []
    cno_by_time = {}  # timestamp -> list of C/No values
    
    for rec in records:
        fields = rec.get('fields_parsed', [])
        utc_time = rec.get('utc_time', 'N/A')
        
        # TRACKSTAT structure:
        # Field 0: solutionStatus
        # Field 1: positionType  
        # Field 2: cutoff
        # Field 3: numberOfChannels
        # Field 4+: satellite data (each satellite has 10 fields)
        # Satellite fields: PRN, glonassFreq, trackingStatus, pseudoRange, doppler, 
        #                   carrierNoiseRatio, lockTime, pseudoRangeResidual, reject, weight
        
        if len(fields) < 4:
            continue
        
        try:
            num_channels = int(float(fields[3])) if fields[3] else 0
            
            # Each satellite has 10 fields starting at field 4
            # C/No is the 6th field of each satellite (index 5 within satellite data)
            satellite_start = 4
            cno_values_this_record = []
            
            for sat_idx in range(num_channels):
                sat_field_start = satellite_start + (sat_idx * 10)
                cno_field_idx = sat_field_start + 5  # carrierNoiseRatio is 6th field (index 5)
                
                if cno_field_idx < len(fields):
                    try:
                        cno = float(fields[cno_field_idx])
                        if cno > 0:  # Valid C/No values are positive
                            all_cno_values.append(cno)
                            cno_values_this_record.append(cno)
                    except (ValueError, IndexError):
                        pass
            
            if cno_values_this_record:
                cno_by_time[utc_time] = cno_values_this_record
        
        except (ValueError, IndexError) as e:
            continue
    
    if not all_cno_values:
        return f"⚠️ **No valid C/No values found in {total_records:,} {log_name} records.**\n\n" \
               "This could indicate:\n" \
               "- No satellites were being tracked\n" \
               "- C/No values are all zero or invalid\n" \
               "- Log parsing issue"
    
    # Compute statistics
    min_cno = min(all_cno_values)
    max_cno = max(all_cno_values)
    avg_cno = sum(all_cno_values) / len(all_cno_values)
    range_cno = max_cno - min_cno
    
    # Find timestamps for min/max
    min_time = None
    max_time = None
    for timestamp, cno_list in cno_by_time.items():
        if min_cno in cno_list and not min_time:
            min_time = timestamp
        if max_cno in cno_list and not max_time:
            max_time = timestamp
    
    # Format based on handler type
    if handler == 'trackstat_cno_max':
        result = f"**Highest C/No in this file: {max_cno:.1f} dB-Hz**\n\n"
        if max_time:
            result += f"**Occurred at:** {max_time}\n\n"
        result += f"**Statistics across all satellites:**\n"
        result += f"- Minimum: {min_cno:.1f} dB-Hz\n"
        result += f"- Average: {avg_cno:.1f} dB-Hz\n"
        result += f"- Range: {range_cno:.1f} dB-Hz\n\n"
        result += f"*Analyzed {len(all_cno_values):,} C/No values from {len(cno_by_time):,} records with {total_records:,} total TRACKSTAT records.*"
    
    elif handler == 'trackstat_cno_min':
        result = f"**Lowest C/No in this file: {min_cno:.1f} dB-Hz**\n\n"
        if min_time:
            result += f"**Occurred at:** {min_time}\n\n"
        result += f"**Statistics across all satellites:**\n"
        result += f"- Maximum: {max_cno:.1f} dB-Hz\n"
        result += f"- Average: {avg_cno:.1f} dB-Hz\n"
        result += f"- Range: {range_cno:.1f} dB-Hz\n\n"
        result += f"*Analyzed {len(all_cno_values):,} C/No values from {len(cno_by_time):,} records with {total_records:,} total TRACKSTAT records.*"
    
    else:  # trackstat_cno_analysis (scintillation detection)
        result = f"**Signal Quality (C/No) Analysis from TRACKSTAT**\n\n"
        result += f"**Log:** {log_name}\n\n"
        result += "| Metric | Value | Timestamp (UTC) |\n"
        result += "|--------|-------|----------------|\n"
        result += f"| **Minimum** | {min_cno:.1f} dB-Hz | {min_time or 'N/A'} |\n"
        result += f"| **Maximum** | {max_cno:.1f} dB-Hz | {max_time or 'N/A'} |\n"
        result += f"| **Average** | {avg_cno:.1f} dB-Hz | - |\n"
        result += f"| **Range** | {range_cno:.1f} dB-Hz | - |\n\n"
        result += f"*Analyzed {len(all_cno_values):,} C/No values from {len(cno_by_time):,} records with {total_records:,} total TRACKSTAT records.*\n\n"
        
        # Scintillation assessment
        result += "**Scintillation Assessment:**\n\n"
        
        if range_cno > 15 or min_cno < 25:
            result += "✅ **Yes, indicators of ionospheric scintillation are present.**\n\n"
            result += "**Evidence:**\n"
            
            if range_cno > 15:
                result += f"- **Signal variation:** C/No range of {range_cno:.1f} dB-Hz indicates amplitude fluctuations\n"
            
            if min_cno < 25:
                result += f"- **Signal fades:** Minimum C/No of {min_cno:.1f} dB-Hz shows signal degradation\n"
            
            if min_cno < 15:
                result += "- **Severe fades:** C/No below 15 dB-Hz indicates severe signal disruption\n"
            
            result += "\n**Typical causes:** Ionospheric irregularities, solar activity, or geomagnetic storms.\n"
            result += "**Impact:** May cause positioning errors and degraded navigation accuracy."
        
        elif range_cno > 10 or min_cno < 35:
            result += "⚠️ **Moderate signal variations detected.**\n\n"
            result += f"The C/No shows moderate variations (range: {range_cno:.1f} dB-Hz, min: {min_cno:.1f} dB-Hz). "
            result += "This could indicate mild scintillation or multipath effects."
        
        else:
            result += "✅ **No significant scintillation detected.**\n\n"
            result += f"The C/No values are stable (range: {range_cno:.1f} dB-Hz, min: {min_cno:.1f} dB-Hz). "
            result += "Signal quality is good across all satellites."
    
    print(f"[FORMAT_TRACKSTAT_CNO] took={time.time()-t0:.3f}s")
    return result

# ── FAST SIGNAL TYPES FORMATTER (NO LLM) ──────────────────────────────
def format_signal_types(question: str, tool_result: dict, intent: dict, handler: str) -> str:
    """
    Ultra-fast signal types listing formatting WITHOUT LLM.
    Parses CHANCONFIGLIST records to extract signal types.
    
    CHANCONFIGLIST structure (from logparsers.js):
    - setInUse: which config is active
    - numOfChanConfig: number of configurations
    - For each config:
      - numOfSignalTypes: number of signal types
      - For each signal type:
        - numChans: number of channels
        - signalType: signal type ID (maps to chanConfigListSignalType enum)
    
    Target: < 500ms
    """
    t0 = time.time()
    
    log_name = tool_result.get('log_name', 'CHANCONFIGLIST')
    total_records = tool_result.get('total_records', 0)
    records = tool_result.get('records', [])
    
    if total_records == 0:
        return f"✅ **No {log_name} records found in this file.**\n\nThis log contains signal type configuration information."
    
    # Parse signal types from records
    signal_types_found = set()
    signal_details = {}
    
    for rec in records:
        fields = rec.get('fields_parsed', [])
        
        # CHANCONFIGLIST has complex nested structure
        # We need to parse it based on the structure from logparsers.js
        # For now, extract signal type IDs from the fields
        
        # Simple approach: look for signal type IDs in the fields
        # Signal type IDs are typically in the range 0-107
        for i, field in enumerate(fields):
            try:
                value = int(float(field))
                # Signal type IDs are 0-107, check if this looks like a signal type
                if 0 <= value <= 107:
                    # This might be a signal type ID
                    signal_types_found.add(value)
            except (ValueError, TypeError):
                pass
    
    if not signal_types_found:
        return f"⚠️ **Could not parse signal types from {total_records:,} {log_name} records.**\n\n" \
               "The CHANCONFIGLIST log structure is complex. Try using the UI's satellite tracking view for detailed signal information."
    
    # Map signal type IDs to names (from NovatelIdConstants.chanConfigListSignalType)
    signal_type_map = {
        0: 'GPSL1', 1: 'GPSL1L2', 4: 'SBASL1', 5: 'GPSL5', 6: 'GPSL1L2C',
        7: 'GPSL1L2AUTO', 8: 'GLOL1L2', 9: 'LBAND', 10: 'GLOL1', 11: 'GALE1',
        12: 'GALE5A', 13: 'GALE5B', 14: 'GALALTBOC', 15: 'BEIDOUB1',
        16: 'GPSL1L2PL2C', 17: 'GPSL1L5', 18: 'SBASL1L5', 19: 'GPSL1L2PL2CL5',
        20: 'GPSL1L2PL5', 21: 'GALE1E5AE5B', 22: 'GALE1E5AE5BALTBOC',
        23: 'GALE1E5A', 24: 'GLOL1L2C', 25: 'GLOL1L2PL2C', 26: 'QZSSL1CA',
        27: 'QZSSL1CAL2C', 28: 'QZSSL1CAL2CL5', 29: 'QZSSL1CAL5',
        30: 'BEIDOUB1B2', 31: 'GALE1E5B', 32: 'BEIDOUB1B3', 33: 'BEIDOUB3',
        34: 'BEIDOUB1B2B3', 35: 'GALE1E5AE5BALTBOCE6', 36: 'GPSL1L2PL2CL5L1C',
        37: 'QZSSL1CAL2CL5L1C', 38: 'QZSSL1CAL2CL5L1CL6', 39: 'GLOL1L3',
        40: 'GLOL3', 41: 'GLOL1L2PL2CL3', 42: 'GPSL1L2PL2CL1C',
        43: 'QZSSL1CAL2CL1C', 44: 'NAVICL5', 45: 'BEIDOUB1C',
        46: 'BEIDOUB1B1C', 47: 'BEIDOUB1B1CB2B3', 48: 'BEIDOUB1B1CB2',
        49: 'BEIDOUB1B2IB2B', 50: 'BEIDOUB1B2B', 51: 'BEIDOUB1B1CB2IB2B',
        52: 'BEIDOUB1B1CB2IB2BB3', 53: 'BEIDOUB1B1CB2B', 54: 'BEIDOUB1B2IB2BB3',
        55: 'BEIDOUB1B2B2B', 56: 'BEIDOUB1B1CB2B2B',
    }
    
    # Group by constellation
    constellations = defaultdict(list)
    for sig_id in sorted(signal_types_found):
        sig_name = signal_type_map.get(sig_id, f"UNKNOWN_{sig_id}")
        
        # Determine constellation
        if sig_name.startswith('GPS'):
            constellations['GPS'].append(sig_name)
        elif sig_name.startswith('GLO'):
            constellations['GLONASS'].append(sig_name)
        elif sig_name.startswith('GAL'):
            constellations['GALILEO'].append(sig_name)
        elif sig_name.startswith('BEIDOU'):
            constellations['BEIDOU'].append(sig_name)
        elif sig_name.startswith('QZSS'):
            constellations['QZSS'].append(sig_name)
        elif sig_name.startswith('SBAS'):
            constellations['SBAS'].append(sig_name)
        elif sig_name.startswith('NAVIC'):
            constellations['NAVIC'].append(sig_name)
        elif sig_name.startswith('LBAND'):
            constellations['L-BAND'].append(sig_name)
        else:
            constellations['OTHER'].append(sig_name)
    
    # Format the result
    result = f"**Signal Types Available in This File**\n\n"
    result += f"Found {len(signal_types_found)} signal type(s) from {total_records:,} {log_name} record(s).\n\n"
    
    # List by constellation
    for constellation in ['GPS', 'GLONASS', 'GALILEO', 'BEIDOU', 'QZSS', 'SBAS', 'NAVIC', 'L-BAND', 'OTHER']:
        if constellation in constellations:
            signals = constellations[constellation]
            result += f"**{constellation}** ({len(signals)} signal type(s)):\n"
            for sig in signals:
                result += f"- {sig}\n"
            result += "\n"
    
    # Filter by constellation if requested
    filter_constellation = intent.get('filter_constellation')
    if filter_constellation and handler == 'constellation_signals':
        if filter_constellation in constellations:
            result = f"**{filter_constellation} Signal Types in This File**\n\n"
            signals = constellations[filter_constellation]
            result += f"Found {len(signals)} {filter_constellation} signal type(s):\n\n"
            for sig in signals:
                result += f"- {sig}\n"
        else:
            result = f"**No {filter_constellation} signals found in this file.**\n\n"
            result += f"Available constellations: {', '.join(constellations.keys())}"
    
    print(f"[FORMAT_SIGNAL_TYPES] took={time.time()-t0:.3f}s")
    return result

# ── Answer formatter (one LLM call) ───────────────────────────────────
_FORMAT_PROMPT = """You are a NovAtel OEM7 log analyst. Answer the user's question using the tool result and the log documentation below.

User question: {question}

Log documentation (field definitions):
{log_docs}

Tool result (JSON):
{tool_result}

FORMATTING GUIDELINES:
- Use **bold** for key findings, log names, and important values (sparingly)
- Use clear paragraph breaks for readability
- Avoid excessive markdown (no ###, ---, ***, ===)
- Use simple bullet points (• or -) for lists
- Keep tables simple with | format, only when needed
- Structure: Brief summary → Details → Conclusion

Instructions:
- For bit_check results: 
  * State the exact matches_found count in bold
  * If matches_found > 0: List ALL utc_time timestamps in chronological order
  * If matches_found = 0: Say "No [subject] detected in this file."
  * Include the field and bit that was checked
- For numeric_stat results: 
  * State min, max, average clearly with units from the documentation
  * Include when min/max occurred (timestamps)
- For raw_listing/summarize results: 
  * Use the field definitions to interpret the fields_parsed arrays
  * Summarize patterns, frequencies, and key findings
  * Do not dump raw field arrays
- Use clear paragraph breaks between sections
- Be precise and factual - never invent data not present in the tool result
- If parse_errors > 0, mention that some records could not be parsed

Example good format:
"**62 spoofing events detected** in the RXSTATUS log (field 4, bit 9).

The events occurred between 2023-05-23 17:41:57 and 17:44:38 UTC, spanning approximately 3 minutes. All 219 RXSTATUS records were checked.

**Event timestamps:**
• 2023-05-23T17:41:57.000Z
• 2023-05-23T17:41:58.000Z
[...]

This indicates active spoofing detection during the observation period."
"""


def format_answer(question: str, tool_result: dict, log_docs: str = "") -> str:
    """Format the answer with optimized token usage for faster LLM response."""
    t0 = time.time()
    
    # Fast path: for bit_check results, format directly without LLM call
    if tool_result.get("status") == "success" and "records_with_bit_set" in tool_result:
        matches = tool_result.get("matches_found", 0)
        log_name = tool_result.get("log_name", "")
        field_idx = tool_result.get("doc_field_index", "")
        bit_pos = tool_result.get("bit_position", "")
        total_checked = tool_result.get("total_checked", 0)

        # Infer the subject (spoofing, jamming, etc.) from the user question
        # so the answer reads naturally instead of generic "events".
        q_lower = (question or "").lower()
        subject_map = [
            ("spoofing",     "spoofing"),
            ("spoof",        "spoofing"),
            ("jamming",      "jamming"),
            ("jammer",       "jamming"),
            ("interference", "interference"),
            ("antenna",      "antenna issue"),
            ("overrun",      "overrun"),
            ("overload",     "overload"),
            ("error",        "error"),
        ]
        subject = next((label for kw, label in subject_map if kw in q_lower), "event")
        subject_title = subject.capitalize()

        if matches == 0:
            result = (
                f"### ❌ No, no {subject} was detected in this file.\n\n"
                f"Checked **{total_checked}** `{log_name}` records "
                f"(field {field_idx}, bit {bit_pos}) — the {subject} bit was never set."
            )
        else:
            records = tool_result["records_with_bit_set"]
            timestamps = [r["utc_time"] for r in records if r.get("utc_time")]

            # Time-range summary
            time_range_line = ""
            if timestamps:
                first_ts, last_ts = timestamps[0], timestamps[-1]
                if first_ts == last_ts:
                    time_range_line = f"- **When:** {first_ts}\n"
                else:
                    time_range_line = f"- **First event:** {first_ts}\n- **Last event:** {last_ts}\n"

            # Compact timestamp list (collapse if very long)
            if len(timestamps) <= 50:
                ts_list = "\n".join(f"- {ts}" for ts in timestamps)
            else:
                ts_list = "\n".join(f"- {ts}" for ts in timestamps[:25])
                ts_list += f"\n\n*… ({len(timestamps) - 50} more timestamps omitted) …*\n\n"
                ts_list += "\n".join(f"- {ts}" for ts in timestamps[-25:])

            result = (
                f"### ✅ Yes, {subject} was detected in this file.\n\n"
                f"**Summary**\n"
                f"- **{subject_title} events:** {matches}\n"
                f"- **Source log:** `{log_name}` (field {field_idx}, bit {bit_pos})\n"
                f"- **Records checked:** {total_checked}\n"
                f"{time_range_line}\n"
                f"**Event timestamps (UTC):**\n\n"
                f"{ts_list}"
            )

        if tool_result.get("parse_errors", 0) > 0:
            result += f"\n\n*Note: {tool_result['parse_errors']} records could not be parsed.*"

        print(f"[FORMAT] fast path took={time.time()-t0:.2f}s")
        return result
    
    # Fast path: for numeric_stat results, format directly without LLM call
    if tool_result.get("status") == "success" and "min_value" in tool_result:
        log_name = tool_result.get("log_name", "")
        field_idx = tool_result.get("doc_field_index", "")
        min_val = tool_result.get("min_value")
        max_val = tool_result.get("max_value")
        avg_val = tool_result.get("average_value")
        range_val = tool_result.get("range")
        total = tool_result.get("total_records", 0)
        valid = tool_result.get("valid_values", 0)
        
        min_time = tool_result.get("min_occurred_at", {}).get("utc_time", "")
        max_time = tool_result.get("max_occurred_at", {}).get("utc_time", "")
        
        # Determine unit from log name (common patterns)
        unit = ""
        if "height" in question.lower() or log_name.upper() == "BESTPOS":
            unit = " m"  # metres for height
        elif "vel" in log_name.lower() or "speed" in question.lower():
            unit = " m/s"
        elif "temp" in log_name.lower():
            unit = "°C"
        
        result = (
            f"**Statistics for {log_name} field {field_idx}:**\n\n"
            f"| Metric | Value | Timestamp (UTC) |\n"
            f"|--------|-------|----------------|\n"
            f"| **Minimum** | {min_val:.3f}{unit} | {min_time} |\n"
            f"| **Maximum** | {max_val:.3f}{unit} | {max_time} |\n"
            f"| **Average** | {avg_val:.3f}{unit} | - |\n"
            f"| **Range** | {range_val:.3f}{unit} | - |\n\n"
            f"Analyzed {valid} valid values from {total} total records."
        )
        
        print(f"[FORMAT] fast path (numeric) took={time.time()-t0:.2f}s")
        return result
    
    # Slow path: use LLM only for raw_listing (complex interpretation needed)
    # But with aggressive token limiting to prevent overflow
    optimized_result = tool_result.copy()
    
    # For raw_listing with many records, provide summary stats instead of all records
    if "records" in optimized_result and len(optimized_result.get("records", [])) > 10:
        records = optimized_result["records"]
        # Keep only first 5 and last 5 records for context
        optimized_result["records"] = records[:5] + records[-5:]
        optimized_result["_sampling_note"] = f"Showing first 5 and last 5 of {len(records)} sampled records"
    
    # Minimal docs - just enough for field interpretation
    trimmed_docs = log_docs[:800] if log_docs else "Not available."
    
    prompt = _FORMAT_PROMPT.format(
        question=question,
        tool_result=json.dumps(optimized_result, indent=2),
        log_docs=trimmed_docs,
    )
    
    try:
        result = get_llm().invoke([HumanMessage(content=prompt)]).content.strip()
        print(f"[FORMAT] LLM path took={time.time()-t0:.2f}s")
        return result
    except Exception as e:
        print(f"[FORMAT] failed: {e}")
        # Fallback: return a simple summary without LLM
        log_name = tool_result.get("log_name", "unknown")
        total = tool_result.get("total_records", 0)
        sample = tool_result.get("sample_size", 0)
        return (
            f"**Analysis of {log_name} log:**\n\n"
            f"Found {total} records in the file (sampled {sample} for analysis).\n\n"
            f"The log contains complex multi-field data. "
            f"For specific analysis, try asking about:\n"
            f"- Specific signal strength values\n"
            f"- Tracking status changes\n"
            f"- Time ranges or specific events\n\n"
            f"*Note: Full analysis unavailable due to data size.*"
        )

# ── Log name selector (uses LLM knowledge + KB) ──────────────────────
_LOG_SELECT_PROMPT = """You are a NovAtel OEM7 expert. Identify which log to query to answer the user's question.

Available logs in the uploaded file:
{available_logs}

User question: {question}

GNSS KNOWLEDGE BASE CONTEXT:
{gnss_kb_excerpt}

NOVATEL LOG SELECTION GUIDE (common patterns):
  - "Jamming" or "jammer" (receiver status bit)  → RXSTATUS
  - "Interference" or "RF interference" (spectrum analysis) → ITDETECTSTATUS
  - Spoofing detection                           → RXSTATUS
  - Position / coordinates / height / lat/lon    → BESTPOS
  - Velocity / speed / heading                   → BESTVEL
  - Satellite tracking / signal quality / C/No   → TRACKSTAT
  - Signal types / constellations configured     → CHANCONFIGLIST
  - Receiver status / errors / flags             → RXSTATUS
  - Time / clock / PPS                           → CLOCKSTEERING, TIMESYNC
  - Differential corrections / RTK               → PSRDIFF, RTCADATA
  - Ionosphere / troposphere                     → IONUTC, TROPMODEL
  - Almanac / ephemeris                          → ALMANAC, GPSEPHEM
  - Hardware / antenna / temperature             → HWMONITOR, ANTENNAPOWER

CRITICAL DISTINCTION:
  - "Jamming" = receiver's internal jamming detection flag → use RXSTATUS
  - "Interference" = detailed RF spectrum analysis → use ITDETECTSTATUS
  - If user asks about "jamming", they want RXSTATUS (not ITDETECTSTATUS)
  - If user asks about "interference", they want ITDETECTSTATUS (not RXSTATUS)
  - "Signal types" or "which signals" = configured signal types → use CHANCONFIGLIST
  - "Signal quality" or "C/No" = carrier-to-noise ratio → use TRACKSTAT

RULES:
1. Choose the log from the available list that best matches the question
2. Pay attention to the EXACT words used - "jamming" ≠ "interference", "signal types" ≠ "signal quality"
3. Use the GNSS knowledge base context above to understand log structures and fields
4. If unsure, prefer RXSTATUS for status/detection questions, BESTPOS for position questions
5. Return ONLY the log name exactly as it appears in the list
6. No explanation, no extra text"""

def extract_log_name(question: str, available_logs: list[str]) -> str | None:
    """
    Use the LLM's own NovAtel knowledge + GNSS KB to pick the right log.
    Constrained to logs actually present in the file.
    Fully generic - works for any log type and any question.
    """
    logs_str = "\n".join(f"- {l}" for l in available_logs)
    
    # Get relevant KB excerpt (first 20K chars for context)
    gnss_kb = get_gnss_kb()
    gnss_kb_excerpt = gnss_kb[:20000] if gnss_kb else "No additional context available."
    
    # Add hint for jamming vs interference distinction
    q_lower = question.lower()
    hint = ""
    if "jam" in q_lower and "interfere" not in q_lower:
        hint = "\n\nHINT: User asked about 'jamming' (not 'interference'), so prefer RXSTATUS over ITDETECTSTATUS."
    elif "interfere" in q_lower and "jam" not in q_lower:
        hint = "\n\nHINT: User asked about 'interference' (not 'jamming'), so prefer ITDETECTSTATUS over RXSTATUS."
    elif "signal type" in q_lower or "which signal" in q_lower or "what signal" in q_lower:
        hint = "\n\nHINT: User asked about 'signal types' (not 'signal quality'), so prefer CHANCONFIGLIST over TRACKSTAT."
    
    prompt = _LOG_SELECT_PROMPT.format(
        question=question, 
        available_logs=logs_str,
        gnss_kb_excerpt=gnss_kb_excerpt
    ) + hint
    
    try:
        response = get_llm().invoke([HumanMessage(content=prompt)])
        log_name = response.content.strip().upper().split()[0].strip(".,\"'-")
        print(f"[LOG_NAME] selected: {log_name} for question: '{question}'")
        return log_name
    except Exception as e:
        print(f"[LOG_NAME] failed: {e}")
        return None


# ── Deterministic Log Pipeline (ZERO LLM for log/field selection) ────
def run_deterministic_pipeline(question: str, session_id: str) -> str:
    """
    FULLY DETERMINISTIC pipeline for log file questions:
      1. Config-based keyword matching picks log + field (NO LLM)
      2. Fetch live NovAtel docs for that log (for answer formatting context only)
      3. Python runs the analysis
      4. Format the answer (fast-path for bit_check/numeric_stat, LLM only for complex)
    
    Target: < 3 seconds end-to-end
    """
    print(f"[DETERMINISTIC] question={question!r}")
    t0 = time.time()
    
    # Step 1: Get file log inventory
    entry = _log_store.get(session_id)
    if not entry or entry["df"].empty:
        clear_status(session_id)
        return "No log file is loaded."
    available_logs = entry["df"]["log_name_raw"].unique().tolist()
    print(f"[DETERMINISTIC] {len(available_logs)} log types in file")
    
    # Step 2: Deterministic mapping (NO LLM)
    set_status(session_id, "Analyzing your question...")
    mapper = get_mapper()
    intent = mapper.extract_intent(question)
    
    print(f"[DETERMINISTIC] intent={intent['use_case']} log={intent['log_type']} "
          f"field={intent['field_index']} bit={intent['bit_position']} "
          f"type={intent['analysis_type']} confidence={intent['confidence']}")
    
    # CONFIDENCE-BASED FALLBACK: If confidence is low, use LLM pipeline instead
    # Lowered threshold from 0.7 to 0.5 to catch more queries deterministically
    CONFIDENCE_THRESHOLD = 0.5
    if intent['confidence'] < CONFIDENCE_THRESHOLD:
        print(f"[DETERMINISTIC] Low confidence ({intent['confidence']:.2f}), falling back to LLM pipeline")
        clear_status(session_id)
        return run_log_pipeline(question, session_id)
    
    # Step 3: Validate log exists in file
    is_valid, error_msg = mapper.validate_against_file(intent, available_logs)
    if not is_valid:
        clear_status(session_id)
        # Add helpful suggestions
        suggestions = mapper.get_use_case_suggestions(available_logs)
        if suggestions:
            error_msg += "\n\n**You can ask:**\n" + "\n".join(f"- {s}" for s in suggestions)
        return error_msg
    
    log_name = intent['log_type']
    field_index = intent['field_index']
    bit_position = intent['bit_position']
    analysis_type = intent['analysis_type']
    special_handler = intent.get('special_handler')
    handler_name = intent.get('handler_name')
    
    # Step 4: Handle direct handlers (fully deterministic, no LLM, no docs needed)
    if analysis_type == 'direct_handler':
        set_status(session_id, f"Processing {handler_name}...")
        
        if handler_name == 'list_logs':
            result = _direct_list_logs(entry)
            clear_status(session_id)
            elapsed = time.time() - t0
            print(f"[DETERMINISTIC] direct handler '{handler_name}' completed in {elapsed:.2f}s")
            return result['result']
        
        elif handler_name == 'time_range':
            result = _direct_time_range(entry)
            clear_status(session_id)
            elapsed = time.time() - t0
            print(f"[DETERMINISTIC] direct handler '{handler_name}' completed in {elapsed:.2f}s")
            return result['result']
        
        elif handler_name == 'data_gaps':
            result = _direct_data_gap(entry)
            clear_status(session_id)
            elapsed = time.time() - t0
            print(f"[DETERMINISTIC] direct handler '{handler_name}' completed in {elapsed:.2f}s")
            return result['result']
        
        else:
            clear_status(session_id)
            return f"Unknown direct handler: {handler_name}"
    
    # Step 5: Run analysis (Python only, no LLM)
    set_status(session_id, f"Analyzing {log_name} data...")
    try:
        if analysis_type == "bit_check" and bit_position is not None:
            result = do_check_bit(session_id, log_name, field_index, bit_position)
        elif analysis_type == "numeric_stat":
            result = do_analyze_field(session_id, log_name, field_index)
        else:
            # raw_listing
            result = do_summarize_log(session_id, log_name, question)
        
        if result.get("status") == "error":
            clear_status(session_id)
            return f"Analysis error: {result['error']}"
        
        # Step 6: Format answer (FAST PATH - no LLM for bit_check/numeric_stat)
        set_status(session_id, "Formatting results...")
        
        # Try fast deterministic formatting first (no LLM, no docs)
        if analysis_type in ('bit_check', 'numeric_stat'):
            answer = format_answer_deterministic(question, result, intent)
            
            # Apply special handlers if needed
            if special_handler == 'scintillation' and analysis_type == 'numeric_stat':
                min_val = result.get("min_value", 0)
                max_val = result.get("max_value", 0)
                range_val = result.get("range", 0)
                
                assessment = "\n\n**Scintillation Assessment:**\n"
                
                if range_val > 40 or min_val < 10:
                    assessment += "✅ **Yes, strong indicators of ionospheric scintillation are present.**\n\n"
                    assessment += "**Evidence:**\n"
                    
                    if range_val > 40:
                        assessment += f"- **Large signal variation:** C/No range of {range_val:.1f} dB-Hz indicates rapid amplitude fluctuations\n"
                    
                    if min_val < 10:
                        assessment += f"- **Deep signal fades:** Minimum C/No of {min_val:.1f} dB-Hz shows severe signal degradation\n"
                    
                    if min_val == 0:
                        assessment += "- **Complete signal loss:** C/No drops to 0 dB-Hz indicate total signal disruption\n"
                    
                    assessment += "\n**Typical causes:** Ionospheric irregularities, solar activity, or geomagnetic storms.\n"
                    assessment += "**Impact:** May cause positioning errors and degraded navigation accuracy."
                
                elif range_val > 20 or min_val < 25:
                    assessment += "⚠️ **Moderate scintillation indicators detected.**\n\n"
                    assessment += f"The C/No shows moderate variations (range: {range_val:.1f} dB-Hz, min: {min_val:.1f} dB-Hz)."
                
                else:
                    assessment += "✅ **No significant scintillation detected.**\n\n"
                    assessment += f"The C/No values are stable (range: {range_val:.1f} dB-Hz, min: {min_val:.1f} dB-Hz)."
                
                answer = answer + assessment
            
            elapsed = time.time() - t0
            print(f"[DETERMINISTIC] completed in {elapsed:.2f}s (FAST PATH - no LLM)")
            clear_status(session_id)
            return answer
        
        # Slow path for raw_listing: fetch docs + use LLM
        else:
            # Special fast handler for interference analysis (ITDETECTSTATUS)
            if special_handler == 'interference_analysis' and analysis_type == 'raw_listing':
                answer = format_interference_analysis(question, result, intent)
                elapsed = time.time() - t0
                print(f"[DETERMINISTIC] completed in {elapsed:.2f}s (FAST PATH - interference handler)")
                clear_status(session_id)
                return answer
            
            # Special handlers for TRACKSTAT C/No queries
            if special_handler in ('trackstat_cno_analysis', 'trackstat_cno_max', 'trackstat_cno_min'):
                answer = format_trackstat_cno(question, result, intent, special_handler)
                elapsed = time.time() - t0
                print(f"[DETERMINISTIC] TRACKSTAT C/No completed in {elapsed:.2f}s")
                clear_status(session_id)
                return answer
            
            # Special handler for signal types listing (CHANCONFIGLIST)
            if special_handler in ('signal_types_list', 'constellation_signals'):
                answer = format_signal_types(question, result, intent, special_handler)
                elapsed = time.time() - t0
                print(f"[DETERMINISTIC] Signal types completed in {elapsed:.2f}s")
                clear_status(session_id)
                return answer
            
            # Default slow path: fetch docs + use LLM
            set_status(session_id, f"Loading {log_name} field definitions...")
            official_docs = fetch_novatel_log_docs(log_name)
            
            answer = format_answer(question, result, log_docs=official_docs)
            
            elapsed = time.time() - t0
            print(f"[DETERMINISTIC] completed in {elapsed:.2f}s (SLOW PATH - used LLM)")
            clear_status(session_id)
            return answer
    
    except Exception as e:
        print(f"[DETERMINISTIC] error: {e}")
        clear_status(session_id)
        return f"An error occurred while analyzing the log: {str(e)}"


# ── Log pipeline (no agent loop) ──────────────────────────────────────
def run_log_pipeline(question: str, session_id: str) -> str:
    """
    Pipeline for log file questions:
      1. LLM picks the right log from file inventory (uses training knowledge, no KB)
      2. Fetch live NovAtel docs for that log (authoritative field/bit table)
      3. LLM extracts exact field + bit from real docs (+ KB as supplementary)
      4. Python runs the analysis
      5. Format the answer
    """
    print(f"[PIPELINE] question={question!r}")
    
    # Determine question intent for dynamic status messages
    q_lower = question.lower()
    intent = "your question"
    if "minimum" in q_lower or "min" in q_lower or "lowest" in q_lower:
        intent = "minimum value query"
    elif "maximum" in q_lower or "max" in q_lower or "highest" in q_lower:
        intent = "maximum value query"
    elif "average" in q_lower or "mean" in q_lower:
        intent = "average calculation"
    elif "range" in q_lower:
        intent = "range analysis"
    elif "scintillation" in q_lower:
        intent = "scintillation detection"
    elif "jamming" in q_lower or "interference" in q_lower:
        intent = "interference analysis"
    elif "spoofing" in q_lower:
        intent = "spoofing detection"
    elif "error" in q_lower or "issue" in q_lower or "problem" in q_lower:
        intent = "error analysis"
    elif "status" in q_lower or "health" in q_lower:
        intent = "status check"
    elif "satellite" in q_lower or "sat" in q_lower:
        intent = "satellite analysis"
    elif "signal" in q_lower or "c/no" in q_lower or "cno" in q_lower:
        intent = "signal quality analysis"
    
    set_status(session_id, f"Analyzing {intent}...")

    # Step 1: get file log inventory
    entry = _log_store.get(session_id)
    if not entry or entry["df"].empty:
        clear_status(session_id)
        return "No log file is loaded."
    available_logs = entry["df"]["log_name_raw"].unique().tolist()
    print(f"[PIPELINE] {len(available_logs)} log types in file")

    # Step 2: LLM picks the log (from its own NovAtel knowledge, constrained to file inventory)
    set_status(session_id, "Determining which log type to analyze...")
    log_name = extract_log_name(question, available_logs)
    if not log_name:
        clear_status(session_id)
        return "Could not determine which log to use for this question."

    # Step 3: Determine question type early to optimize subsequent steps
    q_lower = question.lower()
    is_numeric = any(kw in q_lower for kw in ("min", "max", "average", "mean", "range", "highest", "lowest"))
    
    # Step 4: Parallel fetch of docs and KB search (if needed)
    set_status(session_id, f"Loading {log_name} specifications...")
    import concurrent.futures
    
    official_docs = ""
    kb_elements = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Always fetch docs
        docs_future = executor.submit(fetch_novatel_log_docs, log_name)
        
        # Only fetch KB if needed
        kb_future = None
        if not is_numeric:
            set_status(session_id, f"Searching {log_name} field definitions...")
            subject = extract_subject(question)
            kb_query = f"{log_name} {subject} field bit definition"
            kb_future = executor.submit(kb_search, kb_query, 15)
        
        # Wait for results
        official_docs = docs_future.result()
        if kb_future:
            kb_elements = kb_future.result()
        else:
            print(f"[PIPELINE] Skipping KB search for numeric query")
    
    if official_docs:
        print(f"[PIPELINE] live docs fetched for {log_name}: {len(official_docs)} chars")
    else:
        print(f"[PIPELINE] live docs unavailable for {log_name}, using KB only")

    # Step 5: extract exact field + bit — real docs are primary, KB is fallback
    set_status(session_id, f"Identifying {log_name} field parameters...")
    params = extract_log_params(
        question, kb_elements,
        log_name=log_name,
        official_docs=official_docs,
        top_n=15,
    )
    if not params or "log_name" not in params:
        # Be more helpful - suggest what the user can do
        available_logs = entry["df"]["log_name_raw"].unique().tolist() if entry else []
        
        helpful_msg = (
            "I'm having trouble identifying the exact field to analyze from the documentation. "
            "Let me help you find what you're looking for:\n\n"
        )
        
        if available_logs:
            log_list = ", ".join(available_logs[:8])
            if len(available_logs) > 8:
                log_list += f", and {len(available_logs) - 8} more"
            helpful_msg += f"**Your file contains:** {log_list}\n\n"
        
        helpful_msg += (
            "**Try these approaches:**\n"
            "- Be more specific: 'analyze BESTPOS height field' or 'check RXSTATUS jamming bit'\n"
            "- Ask about common metrics: 'minimum satellites tracked', 'position accuracy', 'signal quality'\n"
            "- List what's available: 'list all logs' or 'show time range'\n\n"
            "What specific information are you looking for?"
        )
        
        return helpful_msg

    log_name      = params.get("log_name")
    field_index   = params.get("field_index")
    bit_position  = params.get("bit_position")
    question_type = params.get("question_type", "raw_listing")

    # Validation
    if not log_name or field_index is None:
        subject = extract_subject(question) if not is_numeric else "the requested field"
        
        # Try to provide helpful guidance instead of just saying "not found"
        available_logs = entry["df"]["log_name_raw"].unique().tolist()
        log_list = ", ".join(available_logs[:10])
        if len(available_logs) > 10:
            log_list += f", and {len(available_logs) - 10} more"
        
        return (
            f"I couldn't identify the exact field for '{subject}' in the available documentation. "
            f"This could mean:\n\n"
            f"1. The field name or log type needs to be more specific\n"
            f"2. The information might be in a different log than expected\n"
            f"3. The documentation for this specific field may not be in the knowledge base\n\n"
            f"**Your file contains these log types:** {log_list}\n\n"
            f"**Suggestions:**\n"
            f"- Try asking about a specific log type (e.g., 'BESTPOS height field' or 'RXSTATUS jamming bit')\n"
            f"- Check if the data you're looking for is in a different log\n"
            f"- Ask 'list all logs' to see what's available in your file\n\n"
            f"I'm here to help - feel free to rephrase your question!"
        )

    try:
        field_index = int(field_index)
        if field_index < 1:
            return f"Invalid field index: {field_index}. Field indices must be >= 1."
    except (TypeError, ValueError):
        return f"Invalid field index from documentation: {field_index!r}"

    print(f"[PIPELINE] → log={log_name} field={field_index} bit={bit_position} type={question_type}")

    # Step 6: Python analysis with proper error handling
    set_status(session_id, f"Computing {log_name} statistics...")
    try:
        if question_type == "bit_check" and bit_position is not None:
            try:
                bit_pos = int(bit_position)
                if bit_pos < 0 or bit_pos > 31:
                    clear_status(session_id)
                    return f"Invalid bit position: {bit_pos}. Must be between 0 and 31."
                result = do_check_bit(session_id, log_name, field_index, bit_pos)
            except (TypeError, ValueError):
                clear_status(session_id)
                return f"Invalid bit position from documentation: {bit_position!r}"
        elif question_type == "numeric_stat":
            result = do_analyze_field(session_id, log_name, field_index)
        else:
            # raw_listing: fetch records and let the formatter interpret them with docs context
            result = do_summarize_log(session_id, log_name, question)

        if result.get("status") == "error":
            clear_status(session_id)
            return f"Analysis error: {result['error']}"

        # Step 7: format answer — pass live docs so formatter can interpret field arrays
        set_status(session_id, "Formatting results...")
        answer = format_answer(question, result, log_docs=official_docs)
        clear_status(session_id)
        return answer
    
    except Exception as e:
        print(f"[PIPELINE] Unexpected error: {e}")
        clear_status(session_id)
        return f"An error occurred while analyzing the log: {str(e)}"

# ── Documentation agent (pure KB, no log tools) ───────────────────────
@tool
def kb_retriever(query: str, max_results: int = MAX_RESULTS) -> dict:
    """Search the NovAtel OEM7 documentation knowledge base."""
    elements = kb_search(query, max_results)
    payload  = {"status": "success", "elements": elements, "total_found": len(elements)}
    _tool_call_log.append({"tool": "kb_retriever", "result": payload})
    return payload

@tool
def context_expander(source_uri: str, element_ids: list = None,
                     page_numbers: list = None,
                     expansion_pages: int = EXPANSION_PAGES) -> dict:
    """Expand context around specific KB elements. Pass csv_source_uri from kb_retriever."""
    t0 = time.time()
    try:
        source_uri = _resolve_data_uri(source_uri)
        df = _download_and_parse(source_uri)
        target_pages: set[int] = set()
        if element_ids:
            for eid in element_ids:
                rows = df[df["element_id"] == eid]
                if not rows.empty:
                    target_pages.update(rows["page_number"].tolist())
        if page_numbers:
            target_pages.update(page_numbers)
        all_pages = {i for p in target_pages
                     for i in range(p - expansion_pages, p + expansion_pages + 1) if i >= 0}
        filtered = df[df["page_number"].isin(all_pages)].copy()
        for col in ["page_number"]:
            if col in filtered.columns:
                filtered[col] = pd.to_numeric(filtered[col], errors="coerce").fillna(0).astype(int)
        for col in [c for c in filtered.columns if c != "page_number"]:
            filtered[col] = filtered[col].fillna("").astype(str)
        available_cols = [c for c in _LLM_COLS if c in filtered.columns]
        slim = filtered[available_cols].to_dict("records") if available_cols else []
        print(f"[EXPANDER] pages={sorted(all_pages)} elements={len(slim)} took={time.time()-t0:.2f}s")
        return {"status": "success", "elements": slim, "total_found": len(slim)}
    except Exception as e:
        print(f"[EXPANDER] error: {e}")
        return {"status": "error", "error": str(e), "elements": [], "total_found": 0}

_DOC_AGENT_PROMPT = """You are a NovAtel OEM7 documentation assistant with deep expertise in GNSS receivers.

BUDGET: 1 kb_retriever call + 1 optional context_expander.

INSTRUCTIONS:
1. Call kb_retriever once with the user's question (use their exact terminology).
2. If you find relevant information, answer directly and cite the log/command name.
3. Only call context_expander if a result is clearly incomplete (cut-off table/definition).
4. If KB search returns no results, use your GNSS/NovAtel expertise to provide a helpful answer:
   - Explain the concept based on standard GNSS/NovAtel knowledge
   - Suggest related logs or commands that might help
   - Recommend checking the official NovAtel OEM7 documentation website
   - Never just say "not available" - always try to be helpful

CRITICAL FORMATTING RULES:
- Write in plain, clean text with clear paragraph breaks
- For emphasis, use **bold** (the UI will render it properly)
- NEVER use # for headings - just write the heading text in **bold** on its own line
- NEVER use ---, ***, ===, or other separator symbols
- Use simple bullet points with - or • (no nested bullets)
- Keep tables minimal with | format only when truly necessary
- Structure: Brief intro → Main content with clear paragraphs → Conclusion

GOOD EXAMPLE:
"**SPAN Configuration Steps**

To configure SPAN, you need to connect and calibrate the IMU with the GNSS receiver.

**1. Connect the IMU**
Use the CONNECTIMU command to establish communication with your inertial measurement unit.

**2. Set IMU Specifications**
Use the SETIMUSPECS command to define the specific IMU model you're using.

The configuration ensures accurate integration of GNSS and inertial data for precise positioning."

BAD EXAMPLE (avoid):
"# SPAN Configuration
***
## Steps
---
### 1. Connect IMU
Use **CONNECTIMU** command..."

RESPONSE STYLE:
- Be knowledgeable and helpful, not dismissive
- If exact documentation isn't found, provide context from your GNSS expertise
- Suggest alternatives or related topics
- Guide users toward solutions

Do not call kb_retriever twice. Start directly with the answer."""

_doc_agent = None

def get_doc_agent():
    global _doc_agent
    if _doc_agent is None:
        _doc_agent = create_react_agent(
            model=get_llm(),
            tools=[kb_retriever, context_expander],
        )
    return _doc_agent

def run_doc_agent(prompt: str, history: list, session_id: str = "") -> str:
    t0       = time.time()
    
    # Extract key topic from question for initial status
    topic = ""
    if session_id:
        p_lower = prompt.lower()
        topics_map = {
            "span": "SPAN configuration", "imu": "IMU setup", "rtk": "RTK configuration",
            "base station": "base station setup", "rover": "rover configuration",
            "heading": "heading configuration", "alignment": "alignment setup",
            "log": "logging configuration", "port": "port configuration",
            "ethernet": "Ethernet setup", "serial": "serial port setup",
            "wifi": "WiFi configuration", "ntrip": "NTRIP configuration",
            "correction": "correction services", "ppp": "PPP configuration",
            "antenna": "antenna setup", "gnss": "GNSS configuration",
            "gps": "GPS setup", "glonass": "GLONASS setup",
            "galileo": "Galileo setup", "beidou": "BeiDou setup",
            "sbas": "SBAS configuration", "bestpos": "BESTPOS message",
            "trackstat": "TRACKSTAT message", "rxstatus": "receiver status",
            "time": "time configuration", "pps": "PPS output",
            "event": "event markers", "trigger": "trigger setup",
        }
        for keyword, topic_name in topics_map.items():
            if keyword in p_lower:
                topic = topic_name
                break
        if not topic:
            if "configure" in p_lower or "setup" in p_lower or "how to" in p_lower:
                topic = "configuration"
            elif "what is" in p_lower or "explain" in p_lower:
                topic = "documentation"
            else:
                topic = "information"
        
        # ── Set an immediate, visible status so the UI shows something right away ──
        set_status(session_id, f"Searching knowledge base for {topic}..." if topic else "Searching knowledge base...")
    
    messages = [SystemMessage(content=_DOC_AGENT_PROMPT)] + history + [HumanMessage(content=prompt)]
    
    try:
        final_answer = ""
        agent = get_doc_agent()
        kb_search_done = False
        
        for event in agent.stream(
            {"messages": messages},
            config={"recursion_limit": 8},
            stream_mode="values"
        ):
            if not session_id or "messages" not in event:
                continue

            last_msg = event["messages"][-1]
            msg_type = last_msg.__class__.__name__

            # ── AI is about to call a tool ────────────────────────────
            if msg_type == "AIMessage":
                tool_calls = getattr(last_msg, "tool_calls", None) or []
                if tool_calls:
                    tool_name = tool_calls[0].get("name", "") if isinstance(tool_calls[0], dict) else getattr(tool_calls[0], "name", "")
                    if tool_name == "kb_retriever":
                        label = f"Searching {topic} documentation..." if topic else "Searching documentation..."
                        set_status(session_id, label)
                    elif tool_name == "context_expander":
                        set_status(session_id, "Expanding context with related documentation...")
                else:
                    # Final AI message (no tool call) — formulating response
                    if last_msg.content:
                        set_status(session_id, "Formulating response...")
                        final_answer = last_msg.content

            # ── Tool result returned ──────────────────────────────────
            elif msg_type == "ToolMessage":
                tool_name = getattr(last_msg, "name", "")
                if tool_name == "kb_retriever":
                    if not kb_search_done:
                        kb_search_done = True
                        set_status(session_id, "Analysing retrieved documentation...")
                elif tool_name == "context_expander":
                    set_status(session_id, "Processing expanded context...")
        
        if not final_answer:
            # Fallback if streaming didn't capture the answer
            set_status(session_id, "Generating answer...")
            result = agent.invoke(
                {"messages": messages},
                config={"recursion_limit": 8},
            )
            final_answer = result["messages"][-1].content
        
        print(f"[DOC_AGENT] took={time.time()-t0:.2f}s")
        if session_id:
            clear_status(session_id)
        return final_answer
        
    except Exception as e:
        print(f"[DOC_AGENT] error: {e}")
        if session_id:
            clear_status(session_id)
        raise

# ── Direct handlers (fully deterministic, zero LLM) ───────────────────
_VALID_TIME_STATUSES = {
    "FINESTEERING","FINE","FINEBACKUPSTEERING","FINEADJUSTING",
    "COARSE","COARSESTEERING","COARSEADJUSTING","FREEWHEELING",
}
def _direct_data_gap(log_entry: dict, gap_threshold: float = 2.0) -> dict:
    """
    Check for time gaps in the file by analysing GPS timestamps across all records.
    Uses the most frequently logged type as the reference signal.
    gap_threshold: seconds — gaps larger than this are reported.
    """
    df = log_entry["df"]
    VALID_TIME = {"FINESTEERING","FINE","FINEBACKUPSTEERING","FINEADJUSTING",
                  "COARSE","COARSESTEERING","COARSEADJUSTING","FREEWHEELING"}

    valid = df[df["time_status"].isin(VALID_TIME) & (df["week"] > 0)].copy()
    if valid.empty:
        return {"result": "No records with valid GPS time found — cannot check for gaps."}

    # Use the most frequent log type as the timing reference (most likely to be continuous)
    most_common_log = valid["log_name_raw"].value_counts().idxmax()
    ref = valid[valid["log_name_raw"] == most_common_log].sort_values("seconds").copy()

    # Convert to absolute GPS seconds (handles week rollovers)
    ref["abs_seconds"] = ref["week"] * 604800 + ref["seconds"]
    ref = ref.sort_values("abs_seconds").reset_index(drop=True)

    # Compute interval between consecutive records
    ref["delta"] = ref["abs_seconds"].diff()

    # Estimate nominal logging rate from median interval
    median_interval = ref["delta"].median()

    # Find gaps — anything more than gap_threshold × nominal interval
    effective_threshold = max(gap_threshold, median_interval * 3)
    gaps = ref[ref["delta"] > effective_threshold].copy()

    if gaps.empty:
        total_duration = ref["abs_seconds"].iloc[-1] - ref["abs_seconds"].iloc[0]
        return {"result": (
            f"**No data gaps detected** in `{log_entry['filename']}`.\n\n"
            f"Reference log: `{most_common_log}` ({len(ref)} records)\n"
            f"Nominal interval: {median_interval:.3f}s | "
            f"Total duration: {total_duration:.1f}s ({total_duration/60:.2f} min)\n"
            f"Data appears continuous throughout."
        )}

    # Format gap details
    lines = []
    for _, row in gaps.iterrows():
        gap_sec = row["delta"]
        gap_start = gps_to_utc_str(int(row["week"]), float(row["seconds"]) - gap_sec)
        gap_end   = gps_to_utc_str(int(row["week"]), float(row["seconds"]))
        lines.append(f"| {gap_start} | {gap_end} | {gap_sec:.2f}s |")

    table = "| Gap Start (UTC) | Gap End (UTC) | Duration |\n|---|---|---|\n" + "\n".join(lines)
    total_duration = ref["abs_seconds"].iloc[-1] - ref["abs_seconds"].iloc[0]
    total_gap = gaps["delta"].sum()

    return {"result": (
        f"**{len(gaps)} data gap(s) detected** in `{log_entry['filename']}`.\n\n"
        f"Reference log: `{most_common_log}` ({len(ref)} records) | "
        f"Nominal interval: {median_interval:.3f}s\n"
        f"Total file duration: {total_duration:.1f}s | "
        f"Total missing time: {total_gap:.1f}s\n\n"
        f"{table}"
    )}

_LIST_TRIGGERS  = ("list all log","what log","all log","log type","how many log",
                   "available log","logs in","logs are","logs present","logs there","what data")
_OVERVIEW_TRIGGERS = ("overview","file overview","give an overview","summarize file",
                      "summary of file","file summary","what's in this file","analyze file",
                      "comprehensive summary","complete summary")
_TIME_TRIGGERS  = ("start time","end time","duration","time range","start and end",
                   "file time","gps time","utc time","how long","begin","finish")
_EVENT_KEYWORDS = ("when","occur","detect","spoof","jam","interfere","bit","status",
                   "error","flag","max","min","average","height","position","velocity",
                   "signal","drop","strength","quality","tracking","lock","loss")

def _direct_list_logs(log_entry: dict) -> dict:
    df         = log_entry["df"]
    log_counts = df.groupby("log_name_raw").size().sort_values(ascending=False)
    lines      = [f"| {n} | {c} |" for n, c in log_counts.items()]
    table      = "| Log Type | Count |\n|---|---|\n" + "\n".join(lines)
    return {"result": f"**{len(log_counts)} log types** in `{log_entry['filename']}` "
                      f"({len(df)} total records):\n\n{table}"}

def _direct_time_range(log_entry: dict) -> dict:
    df    = log_entry["df"]
    valid = df[df["time_status"].isin(_VALID_TIME_STATUSES) & (df["week"] > 0)]
    if valid.empty:
        return {"result": "No records with valid GPS time found in this file."}
    w_s   = int(valid.loc[valid["seconds"].idxmin(), "week"])
    w_e   = int(valid.loc[valid["seconds"].idxmax(), "week"])
    s_s   = float(valid["seconds"].min())
    s_e   = float(valid["seconds"].max())
    weeks = sorted(valid["week"].unique().tolist())
    dur   = (weeks[-1]-weeks[0])*604800+(s_e-s_s) if len(weeks)>1 else s_e-s_s
    return {"result": (
        f"**File time range for `{log_entry['filename']}`:**\n\n"
        f"| | GPS | UTC |\n|---|---|---|\n"
        f"| Start | Week {w_s}, {s_s:.3f}s | {gps_to_utc_str(w_s, s_s)} |\n"
        f"| End   | Week {w_e}, {s_e:.3f}s | {gps_to_utc_str(w_e, s_e)} |\n\n"
        f"**Duration:** {dur:.3f}s ({dur/60:.2f} min)"
    )}

def _direct_file_overview(log_entry: dict) -> dict:
    """
    Comprehensive file overview: logs present, time range, positioning, signal quality, issues.
    Fast deterministic analysis (no LLM).
    """
    df = log_entry["df"]
    filename = log_entry["filename"]
    
    result = f"# File Overview: {filename}\n\n"
    
    # 1. Time Range
    valid = df[df["time_status"].isin(_VALID_TIME_STATUSES) & (df["week"] > 0)]
    if not valid.empty:
        w_s = int(valid.loc[valid["seconds"].idxmin(), "week"])
        w_e = int(valid.loc[valid["seconds"].idxmax(), "week"])
        s_s = float(valid["seconds"].min())
        s_e = float(valid["seconds"].max())
        dur = (w_e - w_s) * 604800 + (s_e - s_s) if w_e > w_s else s_e - s_s
        
        result += f"## ⏱️ Time Range\n"
        result += f"- **Start:** {gps_to_utc_str(w_s, s_s)}\n"
        result += f"- **End:** {gps_to_utc_str(w_e, s_e)}\n"
        result += f"- **Duration:** {dur:.1f}s ({dur/60:.1f} min)\n\n"
    
    # 2. Log Types Present
    log_counts = df.groupby("log_name_raw").size().sort_values(ascending=False)
    result += f"## 📋 Log Types ({len(log_counts)} types, {len(df)} total records)\n\n"
    
    # Group by category
    position_logs = [l for l in log_counts.index if any(x in l for x in ['BESTPOS', 'PSRPOS', 'AVEPOS'])]
    signal_logs = [l for l in log_counts.index if any(x in l for x in ['TRACKSTAT', 'CHANCONFIGLIST', 'SATVIS'])]
    status_logs = [l for l in log_counts.index if any(x in l for x in ['RXSTATUS', 'RXCONFIG', 'VERSION'])]
    ins_logs = [l for l in log_counts.index if any(x in l for x in ['INSPVA', 'INSCONFIG', 'HEADING'])]
    
    if position_logs:
        result += f"**Position:** {', '.join(f'{l} ({log_counts[l]})' for l in position_logs)}\n\n"
    if signal_logs:
        result += f"**Signal Quality:** {', '.join(f'{l} ({log_counts[l]})' for l in signal_logs)}\n\n"
    if ins_logs:
        result += f"**INS/IMU:** {', '.join(f'{l} ({log_counts[l]})' for l in ins_logs)}\n\n"
    if status_logs:
        result += f"**Receiver Status:** {', '.join(f'{l} ({log_counts[l]})' for l in status_logs)}\n\n"
    
    # 3. Quick Position Analysis (if BESTPOS present)
    bestpos_logs = [l for l in log_counts.index if 'BESTPOS' in l]
    if bestpos_logs:
        bestpos_df = df[df["log_name_raw"] == bestpos_logs[0]]
        if not bestpos_df.empty:
            result += f"## 📍 Position Summary (from {bestpos_logs[0]})\n"
            result += f"- {len(bestpos_df)} position records available\n"
            result += f"- Use 'how is positioning status' for detailed analysis\n\n"
    
    # 4. Quick Signal Quality (if TRACKSTAT present)
    trackstat_logs = [l for l in log_counts.index if 'TRACKSTAT' in l]
    if trackstat_logs:
        result += f"## 📡 Signal Quality (from {trackstat_logs[0]})\n"
        result += f"- {log_counts[trackstat_logs[0]]} TRACKSTAT records available\n"
        result += f"- Use 'what is the highest cno' or 'what is the lowest cno' for detailed analysis\n\n"
    
    # 5. Issue Detection (if RXSTATUS present)
    rxstatus_logs = [l for l in log_counts.index if 'RXSTATUS' in l]
    if rxstatus_logs:
        rxstatus_df = df[df["log_name_raw"] == rxstatus_logs[0]]
        if not rxstatus_df.empty:
            result += f"## ⚠️ Issue Detection (from {rxstatus_logs[0]})\n"
            try:
                # rx_status is in the header (column in dataframe)
                issues_found = []
                for _, row in rxstatus_df.head(10).iterrows():  # Check first 10 records
                    rx_status_str = str(row.get("rx_status", "0"))
                    status_word = int(rx_status_str.replace("0x", ""), 16) if rx_status_str.startswith("0x") else int(rx_status_str)
                    
                    if status_word & (1 << 15):  # Bit 15: jamming
                        issues_found.append("Jamming detected")
                        break
                    if status_word & (1 << 9):   # Bit 9: spoofing
                        issues_found.append("Spoofing detected")
                        break
                    if status_word & (1 << 5):   # Bit 5: antenna error
                        issues_found.append("Antenna error")
                        break
                
                if issues_found:
                    result += f"⚠️ **Issues Found:** {', '.join(set(issues_found))}\n\n"
                else:
                    result += f"✅ No major issues detected in first 10 records (jamming, spoofing, antenna errors)\n\n"
            except Exception as e:
                result += f"Status check available - use 'check for jamming' or 'check for spoofing' for details\n\n"
    
    # 6. Suggestions
    result += f"## 💡 Quick Analysis Commands\n"
    result += f"- `what is the highest cno` - Signal quality analysis\n"
    result += f"- `what is the lowest cno` - Weakest signal analysis\n"
    result += f"- `how is positioning status` - Detailed position analysis\n"
    result += f"- `check for jamming` - RF interference detection\n"
    result += f"- `check for spoofing` - Spoofing detection\n"
    result += f"- `list all logs` - Complete log inventory\n"
    
    return {"result": result}

# ── S3 helper ─────────────────────────────────────────────────────────
def upload_to_s3(content: bytes, filename: str) -> str:
    key = f"logs/{filename}"
    get_s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=content)
    return key


# ── Binary pre-processor ──────────────────────────────────────────────
_BINARY_EXTENSIONS = ('.gps', '.bin', '.raw', '.nov', '.novb')

def is_binary_log(filename: str, file_bytes: bytes) -> bool:
    if any(filename.lower().endswith(ext) for ext in _BINARY_EXTENSIONS):
        return True
    if len(file_bytes) >= 3 and file_bytes[:3] == b'\xaa\x44\x12':
        return True
    return False

def convert_binary_to_ascii(file_bytes: bytes, filename: str) -> tuple[bytes, str]:
    import novatel_edie as edie

    ascii_lines = []
    skipped     = 0

    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    fp = None
    try:
        fp = edie.FileParser(tmp_path)

        while True:
            try:
                result = fp.convert(edie.ENCODE_FORMAT.ASCII)
                if isinstance(result, edie.MessageData):
                    line = result.message.decode('utf-8', errors='replace').strip()
                    if line:
                        ascii_lines.append(line)
                else:
                    skipped += 1
            except edie.StreamEmptyException:
                break
            except Exception as e:
                skipped += 1
                continue

    finally:
        del fp  # ✅ release EDIE's file handle first (Windows fix)
        try:
            os.remove(tmp_path)  # now safe to delete
        except Exception as e:
            print(f"[PREPROCESS] Warning: could not delete temp file: {e}")

    ascii_content = '\n'.join(ascii_lines)
    new_filename  = os.path.splitext(filename)[0] + '_converted.ascii'
    print(f"[PREPROCESS] '{filename}' → '{new_filename}' "
          f"({len(ascii_lines)} messages, {skipped} skipped)")
    return ascii_content.encode('utf-8'), new_filename

def preprocess_file(file_bytes: bytes, filename: str) -> tuple[bytes, str]:
    if is_binary_log(filename, file_bytes):
        print(f"[PREPROCESS] Binary detected: '{filename}', converting via EDIE...")
        return convert_binary_to_ascii(file_bytes, filename)
    print(f"[PREPROCESS] ASCII file: '{filename}', no conversion needed.")
    return file_bytes, filename


# ── Main entrypoint ───────────────────────────────────────────────────
@app.entrypoint
async def invoke(payload):
    if isinstance(payload, dict):
        prompt     = payload.get("prompt", "")
        file_b64   = payload.get("file", None)
        s3_key_in  = payload.get("s3_key", None)
        filename   = payload.get("filename", "log.txt")
        session_id = payload.get("session_id", "default-session")
    else:
        prompt = str(payload); file_b64 = s3_key_in = None
        filename = "log.txt"; session_id = "default-session"

    # ── File ingest ───────────────────────────────────────────────────
    if file_b64:
        try:
            file_bytes = base64.b64decode(file_b64)
            file_bytes, filename = preprocess_file(file_bytes, filename)
            if S3_BUCKET and len(file_bytes) > SIZE_THRESHOLD:
                upload_to_s3(file_bytes, filename)
            info = ingest_log_file(file_bytes, filename, session_id)
            return {
                "result": f"Parsed '{info['filename']}': {info['records']} records across "
                          f"{info['log_types']} log types. Ask me anything about this file.",
                "summary": info["summary"],
            }
        except Exception as e:
            return {"result": f"Error parsing log file: {e}"}

    elif s3_key_in:
        try:
            obj  = get_s3_client().get_object(Bucket=S3_BUCKET, Key=s3_key_in)
            info = ingest_log_file(obj["Body"].read(), filename, session_id)
            return {
                "result": f"Parsed '{info['filename']}': {info['records']} records across "
                          f"{info['log_types']} log types. Ask me anything about this file.",
                "summary": info["summary"],
            }
        except Exception as e:
            return {"result": f"Error reading from S3: {e}"}

    # ── Q&A path ─────────────────────────────────────────────────────
    print(f"[QA] prompt={prompt!r} session_id={session_id!r}")
    try:
        apply_guardrail(prompt, source="INPUT")
    except ValueError as e:
        return {"result": str(e)}

    _current_session["id"] = session_id
    _tool_call_log.clear()

    # Check if log file is loaded
    log_entry = _log_store.get(session_id)

    # Load memory history
    history = []
    if MEMORY_ID:
        try:
            events = get_memory_client().list_events(
                memory_id=MEMORY_ID, actor_id=ACTOR_ID,
                session_id=session_id, max_results=10,
            )
            for event in events:
                for item in event.get("payload", []):
                    conv = item.get("conversational", {})
                    role = conv.get("role", "")
                    text = conv.get("content", {}).get("text", "")
                    if role == "USER":
                        history.append(HumanMessage(content=text))
                    elif role == "ASSISTANT":
                        history.append(AIMessage(content=text))
        except Exception as e:
            print(f"Memory retrieve error: {e}")

    history = history[-6:]
    history = [
        msg.__class__(content=msg.content[:800] + "…[truncated]")
        if isinstance(msg.content, str) and len(msg.content) > 800 else msg
        for msg in history
    ]

    # ── Routing ───────────────────────────────────────────────────────
    # log_entry already defined above
    p = prompt.lower()

    # Check if question is clearly non-GNSS/non-technical even with log file loaded
    _NON_GNSS_INDICATORS = (
        "war", "conflict", "battle", "military", "army", "weapon", "soldier",
        "politics", "election", "president", "government", "congress", "parliament",
        "news", "current events", "today", "yesterday", "breaking",
        "weather", "temperature", "rain", "snow", "forecast", "climate",
        "stock", "market", "trading", "investment", "finance", "economy",
        "recipe", "cooking", "food", "restaurant", "meal", "dish",
        "movie", "film", "actor", "actress", "cinema", "netflix",
        "music", "song", "album", "artist", "concert", "band",
        "sports", "football", "basketball", "soccer", "game", "match", "player",
        "medical", "disease", "symptom", "doctor", "hospital", "medicine",
        "travel", "vacation", "hotel", "flight", "tourism", "destination",
        "history", "ancient", "medieval", "century", "historical",
        "programming", "python", "javascript", "code", "software development"
    )
    
    is_non_gnss = any(indicator in p for indicator in _NON_GNSS_INDICATORS)
    
    # If clearly non-GNSS question, route to doc agent (which will use its general knowledge)
    if is_non_gnss and log_entry:
        print("[ROUTE] → doc agent (non-GNSS question, ignoring log file)")
        try:
            output = run_doc_agent(prompt, [], session_id)
        except GraphRecursionError:
            return {"result": "Hit the search budget. Please try rephrasing."}
        except Exception as e:
            return {"result": f"Error: {e}"}
        
        # Save to memory and return
        if MEMORY_ID:
            try:
                get_memory_client().create_event(
                    memory_id=MEMORY_ID, actor_id=ACTOR_ID, session_id=session_id,
                    messages=[(prompt, "USER"), (output, "ASSISTANT")],
                )
            except Exception as e:
                print(f"Memory save error: {e}")
        return {"result": output}

    # 0. Hardcoded scintillation question handler - routes to log pipeline with optimized prompt
    if log_entry and ("scintillation" in p and ("c/no" in p or "cno" in p or "carrier" in p)):
        print("[ROUTE] → scintillation analysis via log pipeline")
        # Rewrite the question to ensure it routes correctly through the log pipeline
        optimized_prompt = "analyze TRACKSTAT field 11 C/No statistics and identify if there is scintillation based on the signal variations"
        try:
            output = run_deterministic_pipeline(optimized_prompt, session_id)
            
            # Enhance the output with scintillation-specific interpretation
            if "Statistics for TRACKSTAT field 11" in output or "TRACKSTAT" in output:
                # Extract statistics from the output to add scintillation assessment
                import re
                min_match = re.search(r'Minimum.*?(\d+\.?\d*)', output)
                max_match = re.search(r'Maximum.*?(\d+\.?\d*)', output)
                range_match = re.search(r'Range.*?(\d+\.?\d*)', output)
                
                if min_match and max_match and range_match:
                    min_val = float(min_match.group(1))
                    max_val = float(max_match.group(1))
                    range_val = float(range_match.group(1))
                    
                    # Add scintillation assessment
                    assessment = "\n\n**Scintillation Assessment:**\n"
                    
                    if range_val > 40 or min_val < 10:
                        assessment += "✅ **Yes, strong indicators of ionospheric scintillation are present.**\n\n"
                        assessment += "**Evidence:**\n"
                        
                        if range_val > 40:
                            assessment += f"- **Large signal variation:** C/No range of {range_val:.1f} dB-Hz indicates rapid amplitude fluctuations characteristic of scintillation\n"
                        
                        if min_val < 10:
                            assessment += f"- **Deep signal fades:** Minimum C/No of {min_val:.1f} dB-Hz shows severe signal degradation\n"
                        
                        if min_val == 0:
                            assessment += "- **Complete signal loss:** C/No drops to 0 dB-Hz indicate periods of total signal disruption\n"
                        
                        if range_val > 50:
                            assessment += "- **Extreme fluctuations:** Range exceeding 50 dB-Hz suggests severe scintillation conditions\n"
                        
                        assessment += "\n**Typical causes:** Ionospheric irregularities (especially in equatorial/auroral regions), solar activity, or geomagnetic storms.\n"
                        assessment += "**Impact:** May cause positioning errors, loss of lock, and degraded navigation accuracy."
                    
                    elif range_val > 20 or min_val < 25:
                        assessment += "⚠️ **Moderate scintillation indicators detected.**\n\n"
                        assessment += f"The C/No shows moderate variations (range: {range_val:.1f} dB-Hz, min: {min_val:.1f} dB-Hz) that suggest some ionospheric disturbance, though not severe."
                    
                    else:
                        assessment += "✅ **No significant scintillation detected.**\n\n"
                        assessment += f"The C/No values are stable (range: {range_val:.1f} dB-Hz, min: {min_val:.1f} dB-Hz), indicating normal signal conditions."
                    
                    output = output + assessment
            
            return {"result": output}
            
        except Exception as e:
            print(f"[SCINTILLATION] error: {e}")
            # Fall through to normal routing
            pass

    # 0a. Hardcoded RXSTATUSEVENT summary handler
    if log_entry and ("rxstatus" in p and ("event" in p or "message" in p) and ("summar" in p or "all" in p or "list" in p)):
        print("[ROUTE] → RXSTATUSEVENT summary")
        try:
            output = run_deterministic_pipeline("summarize all RXSTATUSEVENT records and identify what events occurred", session_id)
            return {"result": output}
        except Exception as e:
            print(f"[RXSTATUSEVENT] error: {e}")
            # Fall through to normal routing
            pass

    # 0b. Hardcoded minimum satellites tracked handler
    if log_entry and ("minimum" in p or "min" in p or "lowest" in p) and ("satellite" in p or "sat" in p) and ("track" in p):
        print("[ROUTE] → minimum satellites tracked")
        try:
            # TRACKSTAT field 2 is typically the number of satellites being tracked
            # But we need to check BESTPOS field for number of satellites used in solution
            # Try BESTPOS first (field 7 = # of satellites in solution)
            result = do_analyze_field(session_id, "BESTPOS", 7)
            
            if result.get("status") == "error":
                # Fallback: try TRACKSTAT if BESTPOS not available
                result = do_analyze_field(session_id, "TRACKSTAT", 2)
            
            if result.get("status") == "error":
                return {"result": f"Could not analyze satellite count: {result['error']}"}
            
            min_val = result.get("min_value", 0)
            max_val = result.get("max_value", 0)
            avg_val = result.get("average_value", 0)
            log_name = result.get("log_name", "")
            field_idx = result.get("doc_field_index", "")
            total = result.get("total_records", 0)
            valid = result.get("valid_values", 0)
            
            min_time = result.get("min_occurred_at", {}).get("utc_time", "")
            max_time = result.get("max_occurred_at", {}).get("utc_time", "")
            
            output = (
                f"**Satellite Tracking Statistics ({log_name} field {field_idx}):**\n\n"
                f"| Metric | Value | Timestamp (UTC) |\n"
                f"|--------|-------|----------------|\n"
                f"| **Minimum** | {int(min_val)} satellites | {min_time} |\n"
                f"| **Maximum** | {int(max_val)} satellites | {max_time} |\n"
                f"| **Average** | {avg_val:.1f} satellites | - |\n\n"
                f"Analyzed {valid} valid values from {total} total records.\n\n"
            )
            
            # Add assessment
            if min_val < 4:
                output += "⚠️ **Warning:** Minimum satellite count is below 4, which is insufficient for 3D positioning. This indicates periods of poor satellite visibility or signal loss.\n"
            elif min_val < 6:
                output += "⚠️ **Caution:** Minimum satellite count is low (4-5 satellites). While sufficient for basic positioning, accuracy may be degraded during these periods.\n"
            else:
                output += "✅ **Good:** Minimum satellite count is adequate for reliable positioning throughout the observation period.\n"
            
            if max_val - min_val > 8:
                output += f"\n**Note:** Large variation in satellite count ({int(max_val - min_val)} satellites) suggests changing sky visibility conditions or signal obstructions."
            
            return {"result": output}
            
        except Exception as e:
            print(f"[MIN_SATELLITES] error: {e}")
            # Fall through to normal routing
            pass

    # 1. Ultra-fast path for simple conversational questions (no KB needed)
    _SIMPLE_CONVERSATIONAL = (
        "hi", "hello", "hey", "thanks", "thank you", "ok", "okay", "yes", "no",
        "good", "great", "nice", "cool", "awesome", "perfect", "got it",
        "i see", "understood", "makes sense", "you are", "you're", "your",
        "bad", "good job", "well done", "terrible", "wrong", "right", "correct",
        "stupid", "smart", "dumb", "clever", "useless", "helpful", "unhelpful"
    )
    
    # Check if it's a very short conversational message or feedback about the agent
    is_about_agent = any(phrase in p for phrase in ["you are", "you're", "your performance", "your answer"])
    is_short_conversational = len(prompt.strip()) < 50 and any(p.startswith(phrase) or p == phrase for phrase in _SIMPLE_CONVERSATIONAL)
    
    if is_short_conversational or is_about_agent:
        print("[ROUTE] → fast conversational response")
        responses = {
            "hi": "Hello! I'm ready to help you analyze NovAtel OEM7 receiver logs. Upload a log file or ask me about NovAtel documentation.",
            "hello": "Hi! I can help you analyze GNSS receiver logs and answer questions about NovAtel OEM7 documentation.",
            "hey": "Hey! Ready to analyze your receiver logs. What would you like to know?",
            "thanks": "You're welcome! Let me know if you need anything else.",
            "thank you": "Happy to help! Feel free to ask more questions.",
            "ok": "Great! What would you like to analyze next?",
            "okay": "Sounds good! Anything else I can help with?",
            "yes": "Understood. What would you like to do?",
            "good": "Glad to help! What's next?",
            "great": "Excellent! Let me know if you need more analysis.",
            "nice": "Thanks! Anything else you'd like to check?",
            "cool": "Great! What else can I analyze for you?",
            "perfect": "Wonderful! Let me know if you need anything else.",
            "got it": "Perfect! Feel free to ask more questions.",
        }
        
        # Handle feedback about the agent
        if is_about_agent:
            if any(word in p for word in ["bad", "wrong", "terrible", "stupid", "useless", "unhelpful"]):
                return {"result": "I apologize if my response wasn't helpful. Could you please clarify what you're looking for? I'm here to help with NovAtel log analysis and GNSS questions."}
            elif any(word in p for word in ["good", "great", "helpful", "smart", "correct", "right"]):
                return {"result": "Thank you! I'm glad I could help. Let me know if you need anything else."}
            else:
                return {"result": "I'm a NovAtel log analysis assistant. How can I help you with your GNSS data?"}
        
        for key, response in responses.items():
            if key in p:
                return {"result": response}
        return {"result": "I'm here to help! What would you like to know?"}

    # 1. Check if this is a general question (not requiring log analysis)
    _GENERAL_QUESTIONS = (
        "what is", "what are", "how does", "how do", "explain", "define",
        "tell me about", "describe", "should i", "is this trustable",
        "is it trustable", "can i trust", "is the data", "trustworthy",
        "reliable", "accuracy", "how accurate", "quality of", "good data",
        "bad data", "valid data", "correct data", "is this good", "is this bad",
        "should i use", "can i use", "is it safe", "safe to use"
    )
    
    is_general = any(phrase in p for phrase in _GENERAL_QUESTIONS)
    
    # If it's a general question without specific log/field reference, use doc agent
    # But if question contains analysis keywords (guess, detect, check, any, find), prefer log pipeline
    _ANALYSIS_KEYWORDS = ("guess", "detect", "check", "any", "find", "identify", 
                          "show", "list", "analyze", "analyse", "scintillation",
                          "jamming", "spoofing", "interference", "error", "issue")
    has_analysis_intent = any(kw in p for kw in _ANALYSIS_KEYWORDS)
    has_specific_reference = any(kw in p for kw in ["in this file", "in the file", "in my file", 
                                                      "of this file", "of the file", "of my file",
                                                      "this log", "the log", "my log", "field", "bit"])
    
    if is_general and not has_specific_reference and not has_analysis_intent and log_entry:
        print("[ROUTE] → doc agent (general question, ignoring file context)")
        try:
            output = run_doc_agent(prompt, history, session_id)
        except GraphRecursionError:
            return {"result": "Hit the search budget. Please try rephrasing."}
        except Exception as e:
            return {"result": f"Error: {e}"}
    # 2. Fully deterministic direct handlers (no LLM)
    elif log_entry:
        is_event = any(kw in p for kw in _EVENT_KEYWORDS)

        # File overview - comprehensive summary
        if any(kw in p for kw in _OVERVIEW_TRIGGERS):
            print("[DIRECT] → file overview")
            set_status(session_id, "Generating comprehensive file overview...")
            result = _direct_file_overview(log_entry)
            clear_status(session_id)
            return result

        # Data gap / continuity check
        _GAP_TRIGGERS = ("gap","missing","continuous","every second","data loss",
                         "time missing","dropped","interval","recording","recorded at",
                         "any second","each second","per second","all second")
        if any(kw in p for kw in _GAP_TRIGGERS):
            print("[DIRECT] → data gap analysis")
            set_status(session_id, "Checking for time gaps in log data...")
            result = _direct_data_gap(log_entry)
            clear_status(session_id)
            return result

        if not is_event and any(kw in p for kw in _LIST_TRIGGERS):
            print("[DIRECT] → list logs")
            set_status(session_id, "Retrieving log inventory from file...")
            result = _direct_list_logs(log_entry)
            clear_status(session_id)
            return result
        if not is_event and any(kw in p for kw in _TIME_TRIGGERS):
            print("[DIRECT] → time range")
            set_status(session_id, "Computing file time range from GPS timestamps...")
            result = _direct_time_range(log_entry)
            clear_status(session_id)
            return result

    # 3. Main routing logic
    try:
        if log_entry:
            # Deterministic log pipeline — config-driven, minimal LLM
            print("[ROUTE] → deterministic pipeline")
            output = run_deterministic_pipeline(prompt, session_id)
        else:
            # Documentation agent — no file loaded
            print("[ROUTE] → doc agent")
            output = run_doc_agent(prompt, history, session_id)

    except GraphRecursionError:
        return {"result": "Hit the search budget. Please try rephrasing."}
    except Exception as e:
        return {"result": f"Error: {e}"}

    try:
        apply_guardrail(output, source="OUTPUT")
    except ValueError as e:
        return {"result": str(e)}

    if MEMORY_ID:
        try:
            get_memory_client().create_event(
                memory_id=MEMORY_ID, actor_id=ACTOR_ID, session_id=session_id,
                messages=[(prompt, "USER"), (output, "ASSISTANT")],
            )
        except Exception as e:
            print(f"Memory save error: {e}")

    return {"result": output}


if __name__ == "__main__":
    app.run()