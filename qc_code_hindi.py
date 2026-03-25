# -*- coding: utf-8 -*-
"""
QC Code SSG — Hindi (STRUCTURAL PARITY VERSION)
Vertex AI Gemini 2.5 Flash

Goal:
- Same logical scaffolding as English QC
- Same safety rails
- Same determinism philosophy
- Hindi-compatible engines
"""

# =================================================
# GLOBAL CACHE
# =================================================
FACT_CACHE = {}

# =================================================
# CORE IMPORTS
# =================================================
import re
import os
import json
import base64
import requests
import hashlib
import tempfile
import html
import streamlit as st
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from difflib import SequenceMatcher
from urllib.parse import urlparse

# =================================================
# GEN AI CLIENT
# =================================================
from google import genai
from google.genai import types as genai_types

# =================================================
# STREAMLIT CONFIG
# =================================================
st.set_page_config(page_title="Hindi Article QC Tool (Gemini)", layout="wide")
st.title("🧪 Hindi Article QC Tool (Gemini 2.5)")
st.caption("Hindi Spelling · Grammar · Editorial Safety · AI Review")

# =================================================
# AUTH CONFIG
# =================================================
PROJECT_ID = "prod-project-jnm-smart-cms"
REGION = "us-central1"
CRED_PATH = "/tmp/gcp_service_account.json"
RULES_PATH = os.path.join(os.path.dirname(__file__), "hindi_qc_rules.txt")
MODEL_FLASH = "gemini-2.5-flash"
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
PROMPT_VERSION_HI = "2026-03-25-4"
PERSISTENT_CACHE_PATH_HI = os.path.join(
    os.path.dirname(__file__),
    ".hindi_ai_output_cache.json",
)

# =================================================
# GCP AUTH
# =================================================
def load_gcp_credentials():
    if "GCP_SERVICE_ACCOUNT_JSON_B64" not in st.secrets:
        st.error("❌ GCP_SERVICE_ACCOUNT_JSON_B64 missing")
        st.stop()

    decoded = base64.b64decode(
        st.secrets["GCP_SERVICE_ACCOUNT_JSON_B64"]
    ).decode("utf-8")

    creds_dict = json.loads(decoded)

    with open(CRED_PATH, "w") as f:
        json.dump(creds_dict, f)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CRED_PATH
    return service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=[CLOUD_PLATFORM_SCOPE],
    )

# =================================================
# DYNAMIC QC RULES (OPTIONAL)
# =================================================
def load_hindi_rules():
    try:
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""
    except Exception:
        return ""

# =================================================
# MODEL INIT (PARALLEL TO ENGLISH)
# =================================================
@st.cache_resource
def init_vertex_and_model():
    creds = load_gcp_credentials()

    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location=REGION,
        credentials=creds,
    )

    # Warmup (exactly like English)
    try:
        client.models.generate_content(
            model=MODEL_FLASH,
            contents="Warmup",
            config=genai_types.GenerateContentConfig(
                temperature=0,
                topP=1,
                maxOutputTokens=8,
            ),
        )
    except Exception:
        pass

    return client

def build_generate_config(generation_config=None):
    cfg = generation_config or {}
    return genai_types.GenerateContentConfig(
        temperature=cfg.get("temperature"),
        topP=cfg.get("top_p", 0),
        topK=cfg.get("top_k"),
        candidateCount=cfg.get("candidate_count"),
        maxOutputTokens=cfg.get("max_output_tokens"),
        seed=0,
        responseMimeType="text/plain",
        thinkingConfig=genai_types.ThinkingConfig(thinkingBudget=0),
    )

def generate_text(prompt, generation_config=None, model_name=MODEL_FLASH):
    client = init_vertex_and_model()
    response = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=build_generate_config(generation_config),
    )
    return response.text or ""

NAVIGATION_TOKENS = {
    "अन्य", "मनोरंजन", "लाइफस्टाइल", "टेक-ज्ञान", "ऑटो", "पॉलिटिक्स",
    "did you know", "एक्सप्लेनर", "लाइव न्यूज़", "लाइव न्यूज़", "शिक्षा",
    "जॉब्स", "कैरियर", "वायरल", "स्पेशल", "वेब स्टोरी", "जागरण इमर्सिव",
}

ARTICLE_ROOT_SELECTORS = [
    "article",
    "[itemprop='articleBody']",
    ".article-content",
    ".article-body",
    ".articleBody",
    ".story-content",
    ".storyBody",
    ".entry-content",
    ".post-content",
    ".content-text",
    ".detail-content",
    ".description",
]

DOMAIN_ARTICLE_SELECTORS = {
    "jagran.com": [
        "article",
        ".ArticleDetail_ArticleDetail__NQJvJ",
        ".jg_m-article",
        "[itemprop='articleBody']",
    ],
    "www.jagran.com": [
        "article",
        ".ArticleDetail_ArticleDetail__NQJvJ",
        ".jg_m-article",
        "[itemprop='articleBody']",
    ],
    "herzindagi.com": [
        "[itemprop='articleBody']",
        "article",
        ".article-content",
        ".story-content",
        ".entry-content",
        ".post-content",
        ".description",
    ],
    "www.herzindagi.com": [
        "[itemprop='articleBody']",
        "article",
        ".article-content",
        ".story-content",
        ".entry-content",
        ".post-content",
        ".description",
    ],
    "onlymyhealth.com": [
        "[itemprop='articleBody']",
        "article",
        ".article-content",
        ".story-content",
        ".entry-content",
        ".post-content",
        ".description",
    ],
    "www.onlymyhealth.com": [
        "[itemprop='articleBody']",
        "article",
        ".article-content",
        ".story-content",
        ".entry-content",
        ".post-content",
        ".description",
    ],
}

EXCLUDED_SUBTREE_SELECTORS = [
    "script",
    "style",
    "noscript",
    "form",
    "nav",
    "figure",
    "figcaption",
    "footer",
    "aside",
    "header",
    ".breadcrumb",
    ".breadcrumbs",
    ".social-share",
    ".share",
    ".author",
    ".byline",
    ".updated",
    ".publish-info",
    ".highlights",
    ".highlight",
    ".keypoints",
    ".key-points",
    ".related",
    ".recommended",
    ".read-more",
    ".also-read",
    ".also_read",
    ".you-may-like",
    ".trending",
    ".copyright",
    ".footer",
    ".ad",
    ".ads",
]

def is_navigation_blob(text: str) -> bool:
    compact = re.sub(r"\s+", " ", text.strip().lower())
    if len(compact) > 180:
        return False

    token_hits = sum(1 for token in NAVIGATION_TOKENS if token in compact)
    return token_hits >= 5

def is_probable_metadata_line(text: str) -> bool:
    compact = re.sub(r"\s+", " ", (text or "").strip())
    lower = compact.lower()

    if not compact:
        return True
    if re.match(r"^https?://", compact):
        return True
    if "www." in lower and lower.endswith(".html"):
        return True
    if lower in {"highlights", "highlight", "हाइलाइट्स"}:
        return True
    if lower.startswith("by ") or lower.startswith("edited by"):
        return True
    if "edited by:" in lower or lower.startswith("updated:") or lower.startswith("published:"):
        return True
    if "all rights reserved" in lower or "copyright" in lower:
        return True
    if "jagran new media" in lower:
        return True
    return False

def should_skip_extracted_text(text: str) -> bool:
    compact = re.sub(r"\s+", " ", text.strip())
    lower = compact.lower()

    if not compact:
        return True
    if is_probable_metadata_line(compact):
        return True
    if lower.startswith("यह भी पढ़ें"):
        return True
    if lower.startswith("...और पढ़ें") or lower.startswith("और पढ़ें"):
        return True
    if lower.startswith("जानिए मुख्य बातें") or "खबर का सार एक नजर में" in lower:
        return True
    if is_navigation_blob(compact):
        return True

    return False

def sanitize_extracted_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    cleaned = re.sub(r"\s*\.{0,3}\s*और पढ़ें\s*$", "", cleaned)
    cleaned = re.sub(r"\s*यह भी पढ़ें[:：].*$", "", cleaned)
    cleaned = re.sub(r"\s*copyright\s+.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*all rights reserved\s*.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def get_domain(url: str) -> str:
    return (urlparse(url).netloc or "").lower()

def is_jagran_domain(url: str) -> bool:
    return get_domain(url) in {"jagran.com", "www.jagran.com"}

def has_inline_read_more(raw_text: str) -> bool:
    compact = re.sub(r"\s+", " ", (raw_text or "").strip())
    return "...और पढ़ें" in compact or compact.endswith("और पढ़ें")

def add_meta_description_summary(soup, url, content, seen):
    if not is_jagran_domain(url):
        return

    meta = (
        soup.find("meta", attrs={"name": "description"})
        or soup.find("meta", attrs={"property": "og:description"})
    )
    if not meta:
        return

    summary = sanitize_extracted_text(meta.get("content", ""))
    if len(summary) < 80:
        return
    if should_skip_extracted_text(summary):
        return
    if summary in seen:
        return

    seen.add(summary)
    content.append(("paragraph", summary))

def build_source_style_notes(source_context: str) -> str:
    domain = get_domain(source_context) if source_context else ""
    if domain in {
        "herzindagi.com",
        "www.herzindagi.com",
        "onlymyhealth.com",
        "www.onlymyhealth.com",
    }:
        return """
Domain style notes:
- In Hindi body copy for lifestyle, home, recipe, and health content, prefer clear Devanagari transliteration for standalone English common nouns or room/object labels when the term is not a brand name and the correction is unambiguous (for example, "Sink" -> "सिंक", "Living Room" -> "लिविंग रूम").
- Normalize broken Unicode half-letter / ZWJ rendering artifacts in Devanagari spellings (for example, forms like "स्‍टाइलिश", "अच्‍छी", "जिन्‍हें") to their standard rendered spellings.
- Use house-style joined compounds where the standard closed form is clear (for example, "रसोईघर").
- Use a hyphen in letter-shape compounds where appropriate (for example, "एल-शेप", "यू-शेप"), but do not add a hyphen to idiomatic expressions where it is not standard.
"""
    return ""

def is_sufficient_article_body(content) -> bool:
    paragraphs = [text for ctype, text in content if ctype == "paragraph"]
    total_chars = sum(len(text) for text in paragraphs)
    return len(paragraphs) >= 2 and total_chars >= 250

def extend_content_from_container(container, content, seen):
    clone = BeautifulSoup(str(container), "html.parser")
    for node in clone.select(",".join(EXCLUDED_SUBTREE_SELECTORS)):
        node.decompose()

    extracted_any = False

    for el in clone.find_all(["p", "li"], recursive=True):
        raw_txt = el.get_text(separator=" ", strip=True)
        if has_inline_read_more(raw_txt):
            continue
        txt = raw_txt
        txt = re.sub(r"\s+([,.;:!?])", r"\1", txt)
        txt = sanitize_extracted_text(txt)
        if not txt or len(txt) < 20:
            continue
        if should_skip_extracted_text(txt):
            continue
        if txt in seen:
            continue
        seen.add(txt)
        content.append(("paragraph", txt))
        extracted_any = True

    for table in clone.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [
                c.get_text(separator=" ", strip=True)
                for c in tr.find_all(["th", "td"])
            ]
            cells = [re.sub(r"\s+", " ", c).strip() for c in cells if c.strip()]
            if not cells:
                continue
            row_text = sanitize_extracted_text(" | ".join(cells))
            if len(row_text) < 5:
                continue
            if should_skip_extracted_text(row_text):
                continue
            if row_text in seen:
                continue
            seen.add(row_text)
            content.append(("table", row_text))
            extracted_any = True

    if extracted_any:
        return

    fallback_text = sanitize_extracted_text(clone.get_text(separator="\n", strip=True))
    for para in re.split(r"\n+", fallback_text):
        para = sanitize_extracted_text(para)
        if len(para) < 20:
            continue
        if should_skip_extracted_text(para):
            continue
        if para in seen:
            continue
        seen.add(para)
        content.append(("paragraph", para))

def extract_from_article_roots(soup, url, content, seen):
    roots = []
    selectors = DOMAIN_ARTICLE_SELECTORS.get(get_domain(url), []) + ARTICLE_ROOT_SELECTORS

    for selector in selectors:
        for node in soup.select(selector):
            text = sanitize_extracted_text(node.get_text(separator=" ", strip=True))
            if len(text) < 150:
                continue
            roots.append(node)

    if not roots:
        return

    ordered = []
    for node in roots:
        if any(node in existing.descendants for existing in ordered):
            continue
        ordered.append(node)

    for node in ordered:
        extend_content_from_container(node, content, seen)

def extract_from_json_article_body(soup, content, seen):
    body_texts = []

    def extract_article_body(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in {"articleBody", "text", "description"} and isinstance(v, str):
                    if len(v) > 80:
                        body_texts.append(v)
                else:
                    extract_article_body(v)
        elif isinstance(obj, list):
            for item in obj:
                extract_article_body(item)

    for script in soup.find_all("script", {"type": "application/ld+json"}):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
            extract_article_body(data)
        except Exception:
            continue

    for body in body_texts:
        cleaned = BeautifulSoup(body, "html.parser").get_text(separator="\n", strip=True)
        for para in re.split(r"\n+|\\n+", cleaned):
            para = sanitize_extracted_text(para)
            if len(para) < 20:
                continue
            if should_skip_extracted_text(para):
                continue
            if para in seen:
                continue
            seen.add(para)
            content.append(("paragraph", para))

# =================================================
# INPUT EXTRACTION (UNCHANGED STRUCTURE)
# =================================================
def clean_docx(file_path):
    from docx import Document
    doc = Document(file_path)

    content = []
    seen = set()

    for para in doc.paragraphs:
        txt = sanitize_extracted_text(para.text)
        if not txt or len(txt) < 15:
            continue
        if should_skip_extracted_text(txt):
            continue
        if txt in seen:
            continue

        seen.add(txt)
        content.append(("paragraph", txt))

    # Extract table rows (if any)
    for table in doc.tables:
        for row in table.rows:
            cells = [c.text.strip() for c in row.cells if c.text and c.text.strip()]
            if not cells:
                continue
            row_text = sanitize_extracted_text(" | ".join(cells))
            if len(row_text) < 5:
                continue
            if should_skip_extracted_text(row_text):
                continue
            if row_text in seen:
                continue
            seen.add(row_text)
            content.append(("table", row_text))

    return content


def clean_article(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    content = []
    seen = set()

    h1 = soup.find("h1")
    if h1:
        content.append(("heading", h1.get_text(strip=True)))
    add_meta_description_summary(soup, url, content, seen)

    structured_content = []
    extract_from_article_roots(soup, url, structured_content, set())
    if is_sufficient_article_body(structured_content):
        return content + structured_content

    json_content = []
    extract_from_json_article_body(soup, json_content, set())
    if is_sufficient_article_body(json_content):
        return content + json_content

    for el in soup.find_all(["p", "li"], recursive=True):
        raw_txt = el.get_text(separator=" ", strip=True)
        if has_inline_read_more(raw_txt):
            continue
        txt = raw_txt
        txt = re.sub(r"\s+([,.;:!?])", r"\1", txt)
        txt = sanitize_extracted_text(txt)

        if not txt or len(txt) < 20:
            continue
        if should_skip_extracted_text(txt):
            continue
        if txt in seen:
            continue

        seen.add(txt)
        content.append(("paragraph", txt))

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [
                c.get_text(separator=" ", strip=True)
                for c in tr.find_all(["th", "td"])
            ]
            cells = [re.sub(r"\s+", " ", c).strip() for c in cells if c.strip()]
            if not cells:
                continue
            row_text = sanitize_extracted_text(" | ".join(cells))
            if len(row_text) < 5:
                continue
            if should_skip_extracted_text(row_text):
                continue
            if row_text in seen:
                continue
            seen.add(row_text)
            content.append(("table", row_text))

    if len(content) < 3:
        extract_from_json_article_body(soup, content, seen)

    return content

# =================================================
# STRUCTURAL LINE CHECK (PARITY WITH ENGLISH)
# =================================================
def is_structural_line_hi(text: str) -> bool:
    t = text.strip()

    if len(t.split()) <= 3:
        return True

    if re.match(r"^\d+[\).]", t):
        return True

    if any(t.startswith(x) for x in [
        "दिन ",
        "नोट:",
        "जानें",
        "देखें",
    ]):
        return True

    return False

# =================================================
# SENTENCE SPLITTER (spaCy replacement)
# =================================================
def split_hindi_sentences(text: str):
    parts = re.split(r"(।|\?|!)", text)
    sentences = []
    buf = ""

    for p in parts:
        buf += p
        if p in {"।", "?", "!"}:
            s = buf.strip()
            if len(s.split()) >= 6:
                sentences.append(s)
            buf = ""

    if buf.strip():
        sentences.append(buf.strip())

    return sentences

# =================================================
# CANONICAL NORMALISERS (EXPLICIT, LIKE ENGLISH)
# =================================================
def canon_hi(text: str) -> str:
    t = text.lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[।,;:!?]", "", t)
    return t

def normalise_hi(text: str) -> str:
    return re.sub(r"[^\w\u0900-\u097F]", "", text.lower())

def normalize_for_match(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("।", " ").replace(".", " ")
    t = re.sub(r"[\"'“”‘’]", "", t)
    t = re.sub(r"[^\w\u0900-\u097F ]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def normalize_quote_style(text: str) -> str:
    return (text or "").translate(str.maketrans({
        "“": '"',
        "”": '"',
        "„": '"',
        "‟": '"',
        "‘": "'",
        "’": "'",
        "‚": "'",
        "‛": "'",
    }))

def normalize_for_equality(text: str) -> str:
    return re.sub(r"\s+", " ", normalize_quote_style((text or "").strip()))

def is_noop_reason(reason: str) -> bool:
    lower = (reason or "").strip().lower()
    return lower in {
        "no corrections needed",
        "no correction needed",
        "no error",
        "no errors",
        "कोई त्रुटि नहीं",
        "कोई गलती नहीं",
    }

def is_noop_correction(original: str, corrected: str) -> bool:
    return normalize_for_equality(original) == normalize_for_equality(corrected)

def is_ignored_styleguide_issue(text: str) -> bool:
    lower = (text or "").strip().lower()
    markers = (
        "prefer chandra-bindu",
        "use chandra-bindu",
        "चंद्रबिंदु का प्रयोग करें",
        "चन्द्रबिन्दु का प्रयोग करें",
    )
    return any(marker in lower for marker in markers)

def should_project_editorial_to_language(issue: str) -> bool:
    lower = (issue or "").strip().lower()
    if not lower:
        return False
    return any(token in lower for token in (
        "spelling",
        "typo",
        "grammar",
        "punctuation",
        "spacing",
        "quotation",
        "quote style for direct speech",
        "sentence ending punctuation",
    ))

def is_no_issue_fact(issue: str, correction: str) -> bool:
    issue_lower = (issue or "").strip().lower()
    correction_lower = (correction or "").strip().lower()
    if issue_lower in {"", "-", "--", "---", "no issue", "no issues"}:
        return True
    if correction_lower in {"", "-", "--", "---", "no issue", "no issues"}:
        return True
    return False

def is_style_only_fact(statement: str, issue: str, correction: str) -> bool:
    lower_issue = (issue or "").strip().lower()
    lower_correction = (correction or "").strip().lower()
    combined = f"{lower_issue} {lower_correction}"

    if canon_hi(statement) == canon_hi(correction):
        return True

    style_markers = (
        "spelling",
        "grammar",
        "punctuation",
        "style",
        "format",
        "wording",
        "terminology",
        "abbreviation",
        "full form",
        "quote",
        "comma",
        "should be",
        "use ",
        "replace ",
        "preferred",
    )
    return any(marker in combined for marker in style_markers)

def find_context_snippet(article_data, needle: str) -> str:
    if not needle:
        return ""

    norm_needle = normalize_for_match(needle)
    for ctype, text in article_data:
        if ctype not in {"paragraph", "table"}:
            continue
        if norm_needle not in normalize_for_match(text):
            continue

        sentences = split_hindi_sentences(text)
        if not sentences:
            sentences = [text]

        for sentence in sentences:
            if norm_needle in normalize_for_match(sentence):
                return sentence.strip()

        return text.strip()

    return ""

def needs_context_expansion(original: str, reason: str) -> bool:
    if not original:
        return False

    token_count = len(original.split())
    lower_reason = (reason or "").lower()
    if token_count <= 2:
        return True
    if len(original) < 18:
        return True
    if "abbreviation" in lower_reason or "full form" in lower_reason:
        return True

    return False

def expand_language_row_context(article_data, original: str, corrected: str, reason: str):
    if not needs_context_expansion(original, reason):
        return original, corrected, reason

    context = find_context_snippet(article_data, original)
    if not context:
        return original, corrected, reason
    if canon_hi(context) == canon_hi(original):
        return original, corrected, reason
    if original not in context:
        return original, corrected, reason

    corrected_context = context.replace(original, corrected, 1)
    if canon_hi(corrected_context) == canon_hi(context):
        return original, corrected, reason

    return context, corrected_context, reason

def batch_hindi_texts(texts, max_chars=6000):
    batches = []
    current = []
    current_len = 0

    for text in texts:
        chunk_len = len(text) + 2
        if current and current_len + chunk_len > max_chars:
            batches.append("\n\n".join(current))
            current = [text]
            current_len = chunk_len
        else:
            current.append(text)
            current_len += chunk_len

    if current:
        batches.append("\n\n".join(current))

    return batches

# =================================================
# VERBATIM ROW FILTER (UNCHANGED LOGIC)
# =================================================
def filter_gemini_rows(raw_table, article_text):
    lines = raw_table.splitlines()
    output = []
    header_added = False
    norm_article = normalize_for_match(article_text)

    for line in lines:
        if line.strip().startswith("| Original"):
            if not header_added:
                output.append("| Original | Corrected | Reason |")
                output.append("|---|---|---|")
                header_added = True
            continue

        if "|" not in line:
            continue

        cols = [c.strip() for c in line.split("|") if c.strip()]
        if len(cols) != 3:
            continue

        original, corrected, reason = cols

        lower_cols = [c.lower() for c in cols]
        if lower_cols == ["original", "corrected", "reason"]:
            continue
        if any(c in {"original", "corrected", "reason"} for c in lower_cols):
            continue
        if any(c.strip() in {"-", "--", "---"} for c in cols):
            continue
        if is_noop_reason(reason) or is_noop_correction(original, corrected):
            continue
        if is_ignored_styleguide_issue(reason):
            continue

        if normalize_for_match(original) in norm_article:
            if not header_added:
                output.append("| Original | Corrected | Reason |")
                output.append("|---|---|---|")
                header_added = True
            output.append(f"| {original} | {corrected} | {reason} |")

    return "\n".join(output) if header_added else ""

# =================================================
# SPELLING VS GRAMMAR CLASSIFIER (EXPLICIT LAYER)
# =================================================
def is_hindi_spelling_issue(original, corrected):
    return (
        len(original.split()) == 1 and
        len(corrected.split()) == 1 and
        original != corrected
    )

def is_spelling_reason(reason: str) -> bool:
    lower = (reason or "").strip().lower()
    return any(marker in lower for marker in (
        "वर्तनी",
        "spelling",
        "typo",
        "misspelling",
    ))

def classify_language_issue(original, corrected, reason):
    if is_spelling_reason(reason):
        return "spelling"
    if is_hindi_spelling_issue(original, corrected):
        return "spelling"
    return "grammar"

def highlight_diff_pair(original: str, corrected: str):
    matcher = SequenceMatcher(a=original or "", b=corrected or "")

    original_parts = []
    corrected_parts = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        original_chunk = (original or "")[i1:i2]
        corrected_chunk = (corrected or "")[j1:j2]

        if tag == "equal":
            original_parts.append(html.escape(original_chunk))
            corrected_parts.append(html.escape(corrected_chunk))
            continue

        if original_chunk:
            original_parts.append(
                f'<span class="qc-diff qc-diff-original">{html.escape(original_chunk)}</span>'
            )
        if corrected_chunk:
            corrected_parts.append(
                f'<span class="qc-diff qc-diff-corrected">{html.escape(corrected_chunk)}</span>'
            )

    return "".join(original_parts), "".join(corrected_parts)

def render_language_table(rows):
    if not rows:
        return ""

    lines = [
        """
<style>
.qc-table {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 1rem;
}
.qc-table th, .qc-table td {
  border: 1px solid rgba(250,250,250,0.14);
  padding: 0.75rem 0.9rem;
  vertical-align: top;
  text-align: left;
}
.qc-table .qc-diff-original {
  color: #ff6b6b;
  font-weight: 700;
}
.qc-table .qc-diff-corrected {
  color: #4ade80;
  font-weight: 700;
}
</style>
<table class="qc-table">
  <thead>
    <tr>
      <th>Original</th>
      <th>Corrected</th>
      <th>Reason</th>
    </tr>
  </thead>
  <tbody>
        """.strip()
    ]

    for original, corrected, reason in rows:
        original_html, corrected_html = highlight_diff_pair(original, corrected)
        lines.append(
            "<tr>"
            f"<td>{original_html}</td>"
            f"<td>{corrected_html}</td>"
            f"<td>{html.escape(reason)}</td>"
            "</tr>"
        )

    lines.append("</tbody></table>")
    return "\n".join(lines)

def split_spelling_grammar_hi(table_md):
    spelling_rows = []
    grammar_rows = []

    rows = re.findall(
        r"\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|",
        table_md
    )

    for o, c, r in rows:
        if o.lower() == "original":
            continue

        reason = r.strip()
        if not reason or reason in {"-", "--", "---"}:
            continue
        if is_noop_reason(reason) or is_noop_correction(o, c):
            continue
        if is_ignored_styleguide_issue(reason):
            continue

        if classify_language_issue(o, c, reason) == "spelling":
            spelling_rows.append((o, c, reason))
        else:
            grammar_rows.append((o, c, reason))

    return spelling_rows, grammar_rows

def parse_language_rows(table_md, article_data=None):
    rows = []
    seen = set()

    matches = re.findall(
        r"\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|",
        table_md or ""
    )

    for original, corrected, reason in matches:
        if original.lower() == "original":
            continue
        if any(x.strip() in {"-", "--", "---"} for x in (original, corrected, reason)):
            continue
        if is_noop_reason(reason) or is_noop_correction(original, corrected):
            continue
        if is_ignored_styleguide_issue(reason):
            continue

        key = (canon_hi(original), canon_hi(corrected), canon_hi(reason))
        if key in seen:
            continue

        seen.add(key)
        original, corrected, reason = (
            original.strip(),
            corrected.strip(),
            reason.strip(),
        )
        if article_data:
            original, corrected, reason = expand_language_row_context(
                article_data, original, corrected, reason
            )
        if is_noop_reason(reason) or is_noop_correction(original, corrected):
            continue
        if is_ignored_styleguide_issue(reason):
            continue
        rows.append((original, corrected, reason))

    return rows

def parse_editorial_rows(editorial_md, article_data=None):
    rows = []
    seen = set()

    matches = re.findall(
        r"\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|",
        editorial_md or ""
    )

    for issue, location, excerpt, corrected in matches:
        lower = [issue.lower(), location.lower(), excerpt.lower(), corrected.lower()]
        if lower == ["issue", "location", "excerpt", "corrected text"]:
            continue
        if issue.lower() == "no issues found":
            continue
        if any(x.strip() in {"-", "--", "---"} for x in (issue, location, excerpt, corrected)):
            continue

        issue, location, excerpt, corrected = (
            issue.strip(),
            location.strip(),
            excerpt.strip(),
            corrected.strip(),
        )
        if is_noop_reason(issue) or is_noop_correction(excerpt, corrected):
            continue
        if is_ignored_styleguide_issue(issue):
            continue
        if article_data:
            excerpt, corrected, _ = expand_language_row_context(
                article_data, excerpt, corrected, issue
            )
        if is_noop_reason(issue) or is_noop_correction(excerpt, corrected):
            continue
        if is_ignored_styleguide_issue(issue):
            continue

        key = (canon_hi(issue), canon_hi(location), canon_hi(excerpt), canon_hi(corrected))
        if key in seen:
            continue

        seen.add(key)
        rows.append((issue, location, excerpt, corrected))

    return rows

def parse_editorial_as_language_rows(editorial_md, article_data=None):
    rows = []
    seen = set()

    for issue, location, excerpt, corrected in parse_editorial_rows(editorial_md, article_data):
        if not should_project_editorial_to_language(issue):
            continue
        key = (canon_hi(excerpt), canon_hi(corrected), canon_hi(issue))
        if key in seen:
            continue

        seen.add(key)
        rows.append((excerpt, corrected, issue))

    return rows

def build_editorial_table(editorial_rows):
    if not editorial_rows:
        return ""

    lines = [
        "| Issue | Location | Excerpt | Corrected Text |",
        "|---|---|---|---|",
    ]
    for issue, location, excerpt, corrected in editorial_rows:
        lines.append(f"| {issue} | {location} | {excerpt} | {corrected} |")

    return "\n".join(lines)

def build_language_tables(language_rows, editorial_rows=None):
    spelling_rows = []
    grammar_rows = []
    seen = set()

    for original, corrected, reason in (language_rows or []) + (editorial_rows or []):
        if not original or not corrected or not reason:
            continue
        if is_noop_reason(reason) or is_noop_correction(original, corrected):
            continue
        if is_ignored_styleguide_issue(reason):
            continue

        key = (canon_hi(original), canon_hi(corrected), canon_hi(reason))
        if key in seen:
            continue

        seen.add(key)
        row = (original, corrected, reason)

        if classify_language_issue(original, corrected, reason) == "spelling":
            spelling_rows.append(row)
        else:
            grammar_rows.append(row)

    return spelling_rows, grammar_rows

# =================================================
# EDITORIAL ROW FILTER (HINDI)
# =================================================
def filter_editorial_rows(raw_table, article_text):
    lines = raw_table.splitlines()
    output = []
    header_added = False
    norm_article = normalize_for_match(article_text)

    for line in lines:
        if line.strip().startswith("| Issue"):
            if not header_added:
                output.append("| Issue | Location | Excerpt | Corrected Text |")
                output.append("|---|---|---|---|")
                header_added = True
            continue

        if "|" not in line:
            continue

        cols = [c.strip() for c in line.split("|") if c.strip()]
        if len(cols) != 4:
            continue

        issue, location, excerpt, corrected = cols

        lower_cols = [c.lower() for c in cols]
        if lower_cols == ["issue", "location", "excerpt", "corrected text"]:
            continue
        if any(c in {"issue", "location", "excerpt", "corrected text"} for c in lower_cols):
            continue
        if any(c.strip() in {"-", "--", "---"} for c in cols):
            continue

        # Excerpt must be present in source text
        if normalize_for_match(excerpt) not in norm_article:
            continue

        # Corrected must be different from excerpt (after trimming)
        if is_noop_reason(issue) or is_noop_correction(excerpt, corrected):
            continue
        if is_ignored_styleguide_issue(issue):
            continue

        if not header_added:
            output.append("| Issue | Location | Excerpt | Corrected Text |")
            output.append("|---|---|---|---|")
            header_added = True
        output.append(f"| {issue} | {location} | {excerpt} | {corrected} |")

    return "\n".join(output) if header_added else ""

# =================================================
# GEMINI GRAMMAR QC (PARAGRAPH PASS)
# =================================================
def gemini_grammar_review(article_data, source_context=""):
    raw_paragraphs = [
        text for ctype, text in article_data
        if (ctype in {"paragraph", "table"})
        and (ctype != "paragraph" or not is_structural_line_hi(text))
    ][:60]

    if not raw_paragraphs:
        return ""

    rules_text = load_hindi_rules()
    rules_block = (
        "\nOptional preferred spellings (hints only; do not limit your review to this list):\n"
        + rules_text + "\n"
        if rules_text else
        "\nOptional preferred spellings: (none provided)\n"
    )
    source_style_notes = build_source_style_notes(source_context)

    BASE_PROMPT = f"""
You are a professional Hindi editor and content QC reviewer.

Scope:
- Review the paragraph carefully from start to end
- Fix spelling, grammar, punctuation, and formatting errors in Hindi
- Do NOT translate, summarize, or rewrite
- Do NOT change names, places, numbers, or quotes unless the spelling is clearly wrong and the correction is unambiguous

Must-follow Hindi editorial rules:
- Use the Hindi danda "।" to end sentences (not a period).
- Use double quotes for direct speech and official statements.
- Use single quotes for titles (books, films, shows, programs, named schemes).
- Straight and curly variants of the same quote type are both acceptable; do not flag quote-shape-only swaps such as "..." vs “...”.
- This publication style does not use chandrabindu in normal spellings where the house style prefers anusvara or the non-chandrabindu form; for example, prefer "पांच" over "पाँच".
- Do not introduce chandrabindu in corrected text unless it is unquestionably required by the publication style.
- For established loanwords that conventionally use the "ऑ" sound, prefer the standard spelling with "ऑ" (for example, "कॉपी", "कॉफी", "कॉलेज") instead of forms like "कापी", "काफी", "कालेज".
- Flag first-mention abbreviation issues only when the short form refers to a named entity
  (such as an organisation, authority, institution, political party, law, scheme, or court)
  and the expansion is genuinely needed for clarity.
- Do not force expansion of common technical abbreviations, scientific labels, measurements,
  or interface tags.
- If a headline/subheading contains क्या/कैसे/क्यों/कब/कितना, it must end with "?".
- Use exactly three dots for ellipsis "...", not more.
- Use numbers 1–9 in words, 10+ in numerals (except dates, time, prices, recipe ingredients).
- Do not use honorifics श्री/श्रीमती for any person name (only "महामहिम" for the President when needed).

{rules_block}
{source_style_notes}

Guidance:
- The preferred spellings list is optional and incomplete. Use it as hints only.
- Still detect and flag other spelling/grammar issues dynamically.
- Treat first-mention abbreviation problems as valid issues only when the short form is unclear
  without expansion in that article context.
- Do not stop after finding the first issue in a paragraph.
- Identify all clear issues in the paragraph, including quote misuse, punctuation, spacing, wording, and abbreviation-introduction problems.
- Apply the house orthography consistently: avoid chandrabindu-style spellings in normal words when the house style prefers non-chandrabindu forms, and preserve standard "ऑ" loanword spellings where clearly appropriate.
- Do not enforce subjective style preferences such as replacing acceptable loanwords,
  banning sentence openings like "लेकिन", or mandating commas after specific discourse markers.
- Do not translate acceptable English technical terms into Hindi just to make a correction.

Constraints:
- Use only TEXT
- Original must be exact substring
- No hallucination

Return output strictly as table with header:
| Original | Corrected | Reason |

Reason must be explicit and non-empty.

TEXT:
"""

    responses = []

    for para in raw_paragraphs:
        try:
            out = generate_text(
                BASE_PROMPT + para,
                generation_config={
                    "temperature": 0,
                    "top_p": 1,
                    "top_k": 1,
                    "candidate_count": 1,
                    "max_output_tokens": 1400
                },
            )
            responses.append(out)
        except Exception:
            continue

    if not responses:
        return ""

    raw = "\n".join(responses)

    matches = re.findall(
        r"\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|",
        raw
    )

    rows = []
    seen = set()

    for o, c, r in matches:
        if o.lower() == "original" or c.lower() == "corrected" or r.lower() == "reason":
            continue
        if any(x.strip() in {"-", "--", "---"} for x in (o, c, r)):
            continue

        key = (canon_hi(o), canon_hi(c), canon_hi(r))
        if key in seen:
            continue

        seen.add(key)
        rows.append(f"| {o} | {c} | {r} |")

    if not rows:
        return ""

    return "\n".join(
        ["| Original | Corrected | Reason |",
         "|---|---|---|"] + rows
    )

# =================================================
# GEMINI EDITORIAL QC (HINDI GUIDELINES)
# =================================================
def gemini_editorial_review_hi(article_data, source_context=""):
    paragraphs = [
        text[:900]
        for ctype, text in article_data
        if ctype == "paragraph"
    ]
    if not paragraphs:
        return ""
    source_style_notes = build_source_style_notes(source_context)

    base_prompt = """
You are an editorial QC reviewer for Hindi content.
Use English for Issue and Corrected Text. Keep fixes concise and specific.

Check for clear violations of these Hindi editorial rules:
- Use the Hindi danda "।" to end sentences (not a period).
- Use double quotes for direct speech; single quotes for titles.
- Straight and curly variants of the same quote type are both acceptable; do not flag quote-shape-only swaps such as "..." vs “...”.
- This publication style avoids chandrabindu in normal house-style spellings; prefer forms like "पांच" over "पाँच".
- For established loanwords with the "ऑ" sound, prefer standard spellings like "कॉपी", "कॉफी", and "कॉलेज" over plain "का/काॅ/का" forms when the correction is unambiguous.
- Flag first-mention abbreviation issues only for named entities or terms that are genuinely unclear without expansion.
- Do not force expansion of common technical abbreviations, scientific labels, measurements, or UI labels.
- If a headline/subheading contains क्या/कैसे/क्यों/कब/कितना, it must end with "?".
- Use exactly three dots for ellipsis "...".
- Use numbers 1–9 in words, 10+ in numerals (except dates, time, prices, recipe ingredients).
- Do not use honorifics श्री/श्रीमती for names (only "महामहिम" for the President when needed).
- Do not enforce subjective style swaps, blanket comma preferences, or bans on sentence openings like "लेकिन"/"और".
- Do not translate acceptable English technical terms just to create a correction.
"""

    if source_style_notes:
        base_prompt += "\n" + source_style_notes.strip() + "\n"

    base_prompt += """

Rules for output:
- Excerpt must be an exact substring from the TEXT.
- Corrected Text must be the fixed version of Excerpt and must differ from Excerpt.
- If the text already follows the rule, do not flag it.
- Do not stop after the first issue; identify all clear issues in the paragraph.

Return output strictly as a markdown table with header:
| Issue | Location | Excerpt | Corrected Text |

If there are no issues, return exactly one row:
| No issues found | - | - | - |
"""

    focused_prompt = """
You are a strict Hindi editorial QC reviewer. This is a focused recall pass.
Use English for Issue and Corrected Text.

Check only these categories, but identify all applicable issues from the paragraph:
- wrong quote type for direct speech (for example, single quotes instead of double quotes), but do not flag straight-vs-curly quote-shape differences
- sentence-ending punctuation or bracket spacing errors
- house-style orthography issues where chandrabindu should not be used
- established loanword spellings that clearly need the "ऑ" form
- abbreviation/acronym used before full form at first mention only when the abbreviation refers to a named entity and expansion is required for clarity
"""

    if source_style_notes:
        focused_prompt += "\n" + source_style_notes.strip() + "\n"

    focused_prompt += """

Rules for output:
- Excerpt must be an exact substring from the TEXT.
- Corrected Text must be the fixed version of Excerpt and must differ from Excerpt.
- Do not flag acceptable technical labels, measurement terms, or stylistic preferences.
- Return all clear issues, even if multiple rows come from the same paragraph.

Return output strictly as a markdown table with header:
| Issue | Location | Excerpt | Corrected Text |

If there are no issues, return exactly one row:
| No issues found | - | - | - |
"""

    responses = []

    for i, para in enumerate(paragraphs, start=1):
        for prompt_template in (base_prompt, focused_prompt):
            prompt = (
                prompt_template
                + f"\nUse the literal location label `Paragraph {i}` for every row from this TEXT.\n\n"
                + "TEXT:\n"
                + para
            )
            try:
                responses.append(
                    generate_text(
                        prompt,
                        generation_config={
                            "temperature": 0,
                            "top_p": 1,
                            "top_k": 1,
                            "candidate_count": 1,
                            "max_output_tokens": 1400,
                        },
                    )
                )
            except Exception:
                continue

    if not responses:
        return ""

    return "\n".join(responses)

# =================================================
# FACT STATEMENT HEURISTICS (EXPLICIT LAYER)
# =================================================
HINDI_FACT_VERBS = [
    "है", "था", "थे", "हैं",
    "कहा", "बताया", "बताई",
    "घोषणा", "जारी", "रिपोर्ट",
    "अनुसार", "मुताबिक"
]

def is_hindi_fact_sentence(sentence):
    return any(v in sentence for v in HINDI_FACT_VERBS)

def extract_fact_statements(article_data):
    statements = []
    seen = set()

    for ctype, text in article_data:
        if ctype not in {"paragraph", "table"}:
            continue

        for sent in split_hindi_sentences(text):
            if not is_hindi_fact_sentence(sent):
                continue

            key = canon_hi(sent)
            if key in seen:
                continue

            seen.add(key)
            statements.append(sent)

    return statements

# =================================================
# FACT CHECK (SECOND PASS, PARITY)
# =================================================
def article_hash(article_data):
    joined = "\n".join(t for _, t in article_data)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()

def analysis_snapshot_key(article_data, source_context=""):
    return f"{PROMPT_VERSION_HI}:{get_domain(source_context)}:{article_hash(article_data)}"

def load_persistent_analysis_cache():
    try:
        with open(PERSISTENT_CACHE_PATH_HI, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def save_persistent_analysis_cache(cache):
    try:
        with open(PERSISTENT_CACHE_PATH_HI, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
    except Exception:
        pass

def load_analysis_snapshot(article_data, source_context=""):
    cache = load_persistent_analysis_cache()
    return cache.get(analysis_snapshot_key(article_data, source_context))

def save_analysis_snapshot(article_data, snapshot, source_context=""):
    cache = load_persistent_analysis_cache()
    cache[analysis_snapshot_key(article_data, source_context)] = snapshot
    save_persistent_analysis_cache(cache)

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def gemini_fact_check(article_data):
    key = article_hash(article_data)
    if key in FACT_CACHE:
        return FACT_CACHE[key]

    statements = extract_fact_statements(article_data)

    if not statements:
        return ""

    full_text = "\n".join(
        text for ctype, text in article_data if ctype in {"paragraph", "table"}
    )

    rows = []
    seen = set()

    for batch in chunked(statements, 5):
        block = "\n".join(f"- {s}" for s in batch)

        PROMPT = f"""
You are an internal factual consistency auditor.

Rules:
- Treat TEXT as closed
- No external knowledge
- Quote exact text
- No paraphrasing
- Only flag direct contradictions, impossible combinations, or statements that are unsupported by the article itself.
- Do not flag style, wording, naming preference, branding simplification, abbreviation expansion, punctuation, or grammar as factual issues.

Return table:
| Statement | Issue | Correct Fact |

TEXT:
{full_text}

STATEMENTS:
{block}
"""

        try:
            out = generate_text(
                PROMPT,
                generation_config={
                    "temperature": 0,
                    "top_p": 1,
                    "max_output_tokens": 512
                },
            )
        except Exception:
            continue

        matches = re.findall(
            r"\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|",
            out
        )

        for s, i, c in matches:
            if s.lower() == "statement" or i.lower() == "issue" or c.lower() == "correct fact":
                continue
            if any(x.strip() in {"-", "--", "---"} for x in (s, i, c)):
                continue
            if is_no_issue_fact(i, c):
                continue
            if is_style_only_fact(s, i, c):
                continue

            sig = (canon_hi(s), canon_hi(i))
            if sig in seen:
                continue

            seen.add(sig)
            rows.append(f"| {s} | {i} | {c} |")

        if len(seen) >= 10:
            break

    if not rows:
        return ""

    result = "\n".join(
        ["| Statement | Issue | Correct Fact |",
         "|---|---|---|"] + rows
    )

    FACT_CACHE[key] = result
    return result

@st.cache_data(show_spinner=False)
def cached_gemini_grammar_review(article_data, source_context=""):
    return gemini_grammar_review(article_data, source_context)

@st.cache_data(show_spinner=False)
def cached_gemini_editorial_review_hi(article_data, source_context=""):
    return gemini_editorial_review_hi(article_data, source_context)

@st.cache_data(show_spinner=False)
def cached_gemini_fact_check(article_data):
    return gemini_fact_check(article_data)

# =================================================
# PIPELINE
# =================================================
def run_pipeline(content):
    return content

# =================================================
# STREAMLIT UI (UNCHANGED STRUCTURE)
# =================================================
st.sidebar.header("Input")
source = st.sidebar.radio("Source", ["URL", "DOCX"])
analyze_clicked = st.sidebar.button("Analyze")
if st.sidebar.button("Clear cached AI outputs"):
    st.cache_data.clear()
    for key in ("article_content", "input_key", "source_context"):
        st.session_state.pop(key, None)

article_content = None
current_key = None
source_context = ""

if source == "URL":
    url = st.sidebar.text_input("Hindi Article URL")
    if url:
        current_key = f"url:{url.strip()}"
        source_context = url.strip()
    if analyze_clicked and url:
        article_content = clean_article(url)
        st.session_state["article_content"] = article_content
        st.session_state["input_key"] = current_key
        st.session_state["source_context"] = source_context
else:
    uploaded = st.sidebar.file_uploader("Upload DOCX", type=["docx"])
    if uploaded:
        file_bytes = uploaded.getvalue()
        current_key = "docx:" + hashlib.sha256(file_bytes).hexdigest()
        if analyze_clicked:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as f:
                f.write(file_bytes)
                article_content = clean_docx(f.name)
            st.session_state["article_content"] = article_content
            st.session_state["input_key"] = current_key
            st.session_state["source_context"] = ""

if article_content is None:
    if current_key and st.session_state.get("input_key") == current_key:
        article_content = st.session_state.get("article_content")
        source_context = st.session_state.get("source_context", source_context)

if article_content:
    qc_content = run_pipeline(article_content)

    st.subheader("📄 Final Article")
    for _, t in qc_content:
        st.write(t)

    st.divider()

    st.subheader("🤖 Gemini QC Review")

    article_text = "\n".join(
        t for c, t in article_content if c in {"paragraph", "table"}
    )

    snapshot = load_analysis_snapshot(qc_content, source_context)
    if snapshot:
        raw = snapshot.get("grammar_raw", "")
        editorial_raw = snapshot.get("editorial_raw", "")
        fact_result = snapshot.get("fact_result", "")
    else:
        raw = cached_gemini_grammar_review(qc_content, source_context)
        editorial_raw = cached_gemini_editorial_review_hi(qc_content, source_context)
        fact_result = cached_gemini_fact_check(qc_content)
        save_analysis_snapshot(
            qc_content,
            {
                "grammar_raw": raw,
                "editorial_raw": editorial_raw,
                "fact_result": fact_result,
            },
            source_context,
        )

    clean = filter_gemini_rows(raw, article_text)
    language_rows = parse_language_rows(clean, qc_content)

    editorial_clean = filter_editorial_rows(editorial_raw, article_text)
    editorial_rows = parse_editorial_rows(editorial_clean, qc_content)
    editorial_language_rows = parse_editorial_as_language_rows(editorial_clean, qc_content)
    editorial_display = build_editorial_table(editorial_rows)

    spelling_table, grammar_table = build_language_tables(
        language_rows,
        editorial_language_rows,
    )

    st.markdown("### ✍️ Spelling Issues")
    if spelling_table:
        st.markdown(render_language_table(spelling_table), unsafe_allow_html=True)
    else:
        st.success("✅ No spelling issues found")

    st.markdown("### 🧠 Grammar Issues")
    if grammar_table:
        st.markdown(render_language_table(grammar_table), unsafe_allow_html=True)
    else:
        st.success("✅ No grammar issues found")

    st.markdown("### 🧠 Gemini Editorial Review")
    if editorial_display:
        st.markdown(editorial_display)
    else:
        st.success("✅ No editorial issues found")

    st.markdown("### 📌 Fact Check")
    if not fact_result or "| Statement |" not in fact_result:
        st.success("✅ No factual issues found")
    else:
        st.markdown(fact_result)
