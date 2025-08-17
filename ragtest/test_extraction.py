#!/usr/bin/env python3
"""
Quick tester for the extract_graph prompt.

- Reads prompts/extract_graph.txt
- Fills placeholders: {entity_types}, {input_text}, {tuple_delimiter}, {record_delimiter}, {completion_delimiter}
- Calls chat model defined in ragtest/settings.yaml (OpenAI-compatible)
- Parses LLM output to structured entities/relationships
- Pretty-prints formatted results for quick inspection

Usage examples:
  python ragtest/test_extract_graph.py --text "患者口干舌燥，舌质红，舌苔黄厚……"
  python ragtest/test_extract_graph.py --input-file ragtest/input/痔诸病.txt

Environment fallback (if settings.yaml is unavailable):
  Set OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_MODEL
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any
from pathlib import Path
import csv

import requests


# ---------------------- Constants & Defaults ----------------------
DEFAULT_SETTINGS_PATH = os.path.join("ragtest", "settings.yaml")
DEFAULT_PROMPT_PATH = os.path.join("ragtest", "prompts", "extract_graph.txt")

# Use rare delimiters to minimize collisions
TUPLE_DELIMITER = "|||"
RECORD_DELIMITER = "\n"
COMPLETION_DELIMITER = "<END>"

# Expected entity types (fallback)
FALLBACK_ENTITY_TYPES = ["症状", "疾病", "体质", "药方", "舌象", "治疗方法", "文献"]


# ---------------------- Utilities ----------------------
def load_settings_yaml(settings_path: str) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception:
        return {}

    if not os.path.exists(settings_path):
        return {}

    with open(settings_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def build_prompt(prompt_template: str, input_text: str, entity_types: List[str]) -> str:
    prompt_filled = (
        prompt_template
        .replace("{entity_types}", ",".join(entity_types))
        .replace("{input_text}", input_text.strip())
        .replace("{tuple_delimiter}", TUPLE_DELIMITER)
        .replace("{record_delimiter}", RECORD_DELIMITER)
        .replace("{completion_delimiter}", COMPLETION_DELIMITER)
    )
    return prompt_filled


def call_chat_completion(
    api_base: str,
    api_key: str,
    model: str,
    prompt: str,
    temperature: float = 0,
    max_tokens: int = 8000,
    top_p: float | None = 1.0,
    request_timeout_s: float = 300.0,
    presence_penalty: float | None = 0.0,
    frequency_penalty: float | None = 0.0,
    seed: int | None = 42,
) -> str:
    url = api_base.rstrip("/") + "/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}" if api_key else None,
    }
    payload: Dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        # DashScope compatible-mode: non-stream calls require enable_thinking=false
        "stream": False,
        "enable_thinking": False,
    }
    if top_p is not None:
        payload["top_p"] = top_p
    if presence_penalty is not None:
        payload["presence_penalty"] = presence_penalty
    if frequency_penalty is not None:
        payload["frequency_penalty"] = frequency_penalty
    if seed is not None:
        payload["seed"] = seed
    # Cut output at our explicit completion token if provider supports stop
    payload["stop"] = [COMPLETION_DELIMITER]

    resp = None
    try:
        resp = requests.post(
            url,
            headers={k: v for k, v in headers.items() if v},
            data=json.dumps(payload),
            timeout=(10, request_timeout_s),
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        status = getattr(resp, "status_code", None) or getattr(e.response, "status_code", "?")
        detail = ""
        source = e.response if getattr(e, "response", None) is not None else resp
        if source is not None:
            try:
                detail = source.text
            except Exception:
                detail = ""
        raise RuntimeError(f"ChatCompletion HTTP {status}: {detail}") from e
    data = resp.json()
    return data["choices"][0]["message"]["content"]


# ---------------------- Parsing ----------------------
@dataclass
class EntityRecord:
    name: str
    type: str
    description: str
    extra: Dict[str, Any]
    source_snippet: str


@dataclass
class RelationshipRecord:
    source: str
    target: str
    rel_type: str
    reason: str
    strength: int
    extra: Dict[str, Any]


KV_PAIR_SPLIT_RE = re.compile(r"[;；]\s*")
KV_RE = re.compile(r"^([A-Za-z_\u4e00-\u9fa5]+)\s*[:：=]\s*(.+)$")
ENTITY_TUPLE_RE = re.compile(r"\(\"entity\".*?\)", flags=re.DOTALL)
REL_TUPLE_RE = re.compile(r"\(\"relationship\".*?\)", flags=re.DOTALL)


def parse_kv_pairs(text: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    if not text:
        return props
    # split by Chinese/English semicolon
    for part in KV_PAIR_SPLIT_RE.split(text):
        part = part.strip()
        if not part:
            continue
        m = KV_RE.match(part)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            if key:
                props[key] = val
    return props


def extract_source_snippet(description: str) -> Tuple[str, str]:
    if not description:
        return "", ""
    m = re.search(r"来源原文片段[:：]\s*([^\n]{0,120})", description)
    snippet = m.group(1).strip() if m else ""
    if m:
        # Remove that segment
        start, end = m.span()
        # also remove any preceding delimiter like '；'
        desc = (description[:max(0, start - 1)] + description[end:]).strip()
        if desc.endswith("；") or desc.endswith(";"):
            desc = desc[:-1]
        return desc.strip(), snippet
    return description.strip(), ""


def parse_entity_tuple(raw: str) -> EntityRecord:
    # Format: ("entity"|||<name>|||<type>|||<description>)
    inner = raw.strip()[1:-1]  # remove surrounding parentheses
    # Remove leading "entity" and delimiter
    assert inner.startswith("\"entity\"")
    inner = inner[len("\"entity\"") :]
    if inner.startswith(TUPLE_DELIMITER):
        inner = inner[len(TUPLE_DELIMITER) :]
    parts = inner.split(TUPLE_DELIMITER)
    if len(parts) < 3:
        raise ValueError(f"Bad entity tuple: {raw}")
    name, e_type = parts[0].strip(), parts[1].strip()
    description = TUPLE_DELIMITER.join(parts[2:]).strip()
    description, snippet = extract_source_snippet(description)
    extra = parse_kv_pairs(description)
    return EntityRecord(name=name, type=e_type, description=description, extra=extra, source_snippet=snippet)


def parse_relationship_tuple(raw: str) -> RelationshipRecord:
    # Format: ("relationship"|||<source>|||<target>|||<description>|||<strength>)
    inner = raw.strip()[1:-1]
    assert inner.startswith("\"relationship\"")
    inner = inner[len("\"relationship\"") :]
    if inner.startswith(TUPLE_DELIMITER):
        inner = inner[len(TUPLE_DELIMITER) :]
    parts = inner.split(TUPLE_DELIMITER)
    if len(parts) < 4:
        raise ValueError(f"Bad relationship tuple: {raw}")
    source, target = parts[0].strip(), parts[1].strip()
    description = TUPLE_DELIMITER.join(parts[2:-1]).strip()
    strength_text = parts[-1].strip()
    try:
        strength = int(re.findall(r"\d+", strength_text)[0])
    except Exception:
        strength = 0

    # Extract [type] prefix and remaining reason
    rel_type = ""
    reason = description
    m = re.match(r"\s*\[([^\]]+)\]\s*(.*)$", description)
    if m:
        rel_type = m.group(1).strip()
        reason = m.group(2).strip()

    extra = parse_kv_pairs(reason)
    return RelationshipRecord(source=source, target=target, rel_type=rel_type, reason=reason, strength=strength, extra=extra)


def parse_output(raw_output: str) -> Tuple[List[EntityRecord], List[RelationshipRecord]]:
    # Respect completion delimiter if present
    if COMPLETION_DELIMITER in raw_output:
        raw_output = raw_output.split(COMPLETION_DELIMITER)[0]

    entities: List[EntityRecord] = []
    relationships: List[RelationshipRecord] = []

    # Robust multi-line tuple extraction to avoid loss on wrapped lines
    entity_tuples = [m.group(0) for m in ENTITY_TUPLE_RE.finditer(raw_output)]
    rel_tuples = [m.group(0) for m in REL_TUPLE_RE.finditer(raw_output)]

    for t in entity_tuples:
        # Normalize stray newlines/whitespace inside tuple to avoid split issues
        normalized = re.sub(r"\s+", " ", t.strip())
        try:
            entities.append(parse_entity_tuple(normalized))
        except Exception:
            # Skip bad tuples but continue
            pass

    for t in rel_tuples:
        normalized = re.sub(r"\s+", " ", t.strip())
        try:
            relationships.append(parse_relationship_tuple(normalized))
        except Exception:
            pass

    return entities, relationships


# ---------------------- Post-processing ----------------------
def normalize_relationship_directions(
    entities: List[EntityRecord], relationships: List[RelationshipRecord]
) -> Tuple[List[RelationshipRecord], List[Tuple[str, str, str]]]:
    """Auto-correct common arrow mistakes based on allowed type directions.

    Returns adjusted relationships and a list of (src, rel, tgt) that were flipped.
    """
    name_to_type: Dict[str, str] = {}
    for e in entities:
        if e.name not in name_to_type:
            name_to_type[e.name] = e.type

    # Allowed directions definition (source_types, target_types) as sets or None to skip
    def S(*types: str) -> set[str]:
        return set(types)

    REL_DIR_RULES: Dict[str, Tuple[set[str] | None, set[str] | None]] = {
        "表现为": (S("症状"), S("舌象")),
        "可见于": (S("舌象"), S("疾病")),
        "导致": (S("体质"), S("疾病")),
        "治疗": (S("药方", "治疗方法"), S("疾病")),
        "适用于": (S("药方", "治疗方法"), S("疾病")),
        "来源于": (None, S("文献")),
        # "相关": any → any (do not enforce)
        # 对于用户新增的“属于”不做强纠正，交由上游或属性表达
    }

    corrected: List[Tuple[str, str, str]] = []
    adjusted: List[RelationshipRecord] = []
    for r in relationships:
        rule = REL_DIR_RULES.get(r.rel_type)
        if not rule or r.rel_type == "相关":
            adjusted.append(r)
            continue
        src_t = name_to_type.get(r.source, "")
        tgt_t = name_to_type.get(r.target, "")
        src_ok = (rule[0] is None) or (src_t in rule[0])
        tgt_ok = (rule[1] is None) or (tgt_t in rule[1])
        if src_ok and tgt_ok:
            adjusted.append(r)
            continue
        # Try flipping if reversed matches
        rev_src_ok = (rule[0] is None) or (tgt_t in rule[0])
        rev_tgt_ok = (rule[1] is None) or (src_t in rule[1])
        if rev_src_ok and rev_tgt_ok:
            corrected.append((r.source, r.rel_type, r.target))
            adjusted.append(
                RelationshipRecord(
                    source=r.target,
                    target=r.source,
                    rel_type=r.rel_type,
                    reason=f"[方向已自动纠正] {r.reason}" if r.reason else "",
                    strength=r.strength,
                    extra=r.extra,
                )
            )
        else:
            adjusted.append(r)

    return adjusted, corrected


def pretty_print_results(entities: List[EntityRecord], relationships: List[RelationshipRecord]) -> None:
    print("\n===== 提取结果 =====")
    print(f"实体数: {len(entities)}  |  关系数: {len(relationships)}")

    # Entities by type
    by_type: Dict[str, List[EntityRecord]] = {}
    for e in entities:
        by_type.setdefault(e.type, []).append(e)

    for e_type, items in sorted(by_type.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        print(f"\n--- 实体类型: {e_type}  (共 {len(items)})")
        for e in items:
            short_desc = (e.description[:120] + "…") if len(e.description) > 120 else e.description
            print(f"  - 名称: {e.name}")
            print(f"    描述: {short_desc}")
            if e.source_snippet:
                print(f"    原文: {e.source_snippet}")
            if e.extra:
                kv_preview = "; ".join(f"{k}={v}" for k, v in list(e.extra.items())[:6])
                print(f"    属性: {kv_preview}")

    if relationships:
        print("\n--- 关系 (前 30 条)")
        for r in relationships[:30]:
            reason_short = (r.reason[:140] + "…") if len(r.reason) > 140 else r.reason
            print(f"  - {r.source} -[{r.rel_type or 'RELATED_TO'}]-> {r.target}  (强度={r.strength})")
            if reason_short:
                print(f"    理由: {reason_short}")
            if r.extra:
                kv_preview = "; ".join(f"{k}={v}" for k, v in list(r.extra.items())[:6])
                print(f"    属性: {kv_preview}")


def preview_raw_output(raw_text: str, save_dir: Path) -> None:
    """Show head/tail preview and persist full text to a file for inspection."""
    save_dir.mkdir(parents=True, exist_ok=True)
    raw_file = save_dir / "last_extract_raw.txt"
    raw_file.write_text(raw_text, encoding="utf-8")

    print("\n===== 原始模型输出（前后各 1500 字预览） =====")
    head = raw_text[:1500]
    tail = raw_text[-1500:]
    print("-- 头部 --\n" + head)
    print("\n-- 尾部 --\n" + tail)
    print(f"\n💾 完整原始输出已保存到: {raw_file}")

    # Tuple level preview (counts and first few)
    entity_tuples = re.findall(r"\(\"entity\".*?\)", raw_text, flags=re.DOTALL)
    rel_tuples = re.findall(r"\(\"relationship\".*?\)", raw_text, flags=re.DOTALL)
    print(f"\nRaw tuple计数: entity={len(entity_tuples)}, relationship={len(rel_tuples)}")
    if entity_tuples:
        print("实体tuple示例(最多5条):")
        for t in entity_tuples[:5]:
            snippet = t
            if len(snippet) > 300:
                snippet = snippet[:300] + "…"
            print("  " + snippet)
    if rel_tuples:
        print("关系tuple示例(最多5条):")
        for t in rel_tuples[:5]:
            snippet = t
            if len(snippet) > 300:
                snippet = snippet[:300] + "…"
            print("  " + snippet)


# ---------------------- Validation ----------------------
# 移除关系类型白名单限制，允许所有关系类型
# ALLOWED_REL_TYPES = {"表现为", "可见于", "导致", "治疗", "来源于", "相关", "适用于"}

# 舌象四项必填与枚举
TONGUE_REQUIRED_KEYS = ("颜色", "形态", "舌苔", "苔润燥")
TONGUE_TAI_ALLOWED = {"薄", "厚", "无苔"}
TONGUE_RUNZAO_ALLOWED = {"润", "燥"}

# 九大体质
ALLOWED_CONSTITUTIONS = {
    "平和质", "气虚质", "阳虚质", "阴虚质", "痰湿质", "湿热质", "血瘀质", "气郁质", "特禀质"
}

# 对实体键不再严格白名单校验，仅保留必要项的检查（疾病.病因、舌象四项、体质名称）


def validate_results(entities: List[EntityRecord], relationships: List[RelationshipRecord], allowed_entity_types: List[str]) -> Dict[str, Any]:
    report: Dict[str, Any] = {}

    # Entities index by name
    name_to_type: Dict[str, str] = {}
    for e in entities:
        if e.name not in name_to_type:
            name_to_type[e.name] = e.type

    # Entity validations
    invalid_entity_type = [e for e in entities if e.type not in allowed_entity_types]
    empty_type_or_desc = [e for e in entities if (not e.type or not e.description.strip())]

    # 舌象必须包含4项关键属性，且枚举值合法
    tongue_required_missing: List[Tuple[str, List[str]]] = []
    tongue_enum_errors: List[Tuple[str, Dict[str, str]]] = []
    for e in entities:
        if e.type != "舌象":
            continue
        missing = [k for k in TONGUE_REQUIRED_KEYS if not str(e.extra.get(k, "")).strip()]
        if missing:
            tongue_required_missing.append((e.name, missing))
        # Enum checks
        tai = str(e.extra.get("舌苔", "")).strip()
        runzao = str(e.extra.get("苔润燥", "")).strip()
        bad: Dict[str, str] = {}
        if tai and tai not in TONGUE_TAI_ALLOWED:
            bad["舌苔"] = tai
        if runzao and runzao not in TONGUE_RUNZAO_ALLOWED:
            bad["苔润燥"] = runzao
        if bad:
            tongue_enum_errors.append((e.name, bad))

    # 疾病必须有病因
    disease_missing_etiology = [
        e for e in entities
        if e.type == "疾病" and ("病因" not in e.extra or not str(e.extra.get("病因")).strip())
    ]

    # 体质名称必须属于九大体质
    constitution_invalid = [
        e for e in entities
        if e.type == "体质" and e.name not in ALLOWED_CONSTITUTIONS
    ]

    # 不再校验实体未知键，减少对键名数量/命名的约束

    # Relationship validations
    # 移除关系类型白名单验证，允许所有关系类型
    invalid_rel_type = []  # [r for r in relationships if r.rel_type not in ALLOWED_REL_TYPES]

    def get_type(name: str) -> str:
        return name_to_type.get(name, "")

    arrow_errors: List[Tuple[RelationshipRecord, str]] = []
    for r in relationships:
        st, tt = get_type(r.source), get_type(r.target)
        if not st or not tt:
            # Skip direction check if missing entities
            continue
        ok = True
        reason = ""
        if r.rel_type == "表现为":
            # 两种合法方向：症状→舌象；疾病→症状
            ok = (st == "症状" and tt == "舌象") or (st == "疾病" and tt == "症状")
            reason = "应为 症状→舌象 或 疾病→症状"
        elif r.rel_type == "可见于":
            ok = (st == "舌象" and tt == "疾病")
            reason = "应为 舌象→疾病"
        elif r.rel_type == "导致":
            ok = (st == "体质" and tt == "疾病")
            reason = "应为 体质→疾病"
        elif r.rel_type == "治疗":
            ok = (st in {"药方", "治疗方法"} and tt == "疾病")
            reason = "应为 (药方/治疗方法)→疾病"
        elif r.rel_type == "适用于":
            ok = (st in {"药方", "治疗方法"} and tt == "疾病")
            reason = "应为 (药方/治疗方法)→疾病"
        elif r.rel_type == "来源于":
            ok = (tt == "文献")
            reason = "目标应为 文献"
        elif r.rel_type == "相关":
            ok = True
        else:
            # 对于其他关系类型，不进行严格的方向验证，都认为是合法的
            ok = True
            reason = ""

        if not ok:
            arrow_errors.append((r, reason))

    report["invalid_entity_type_count"] = len(invalid_entity_type)
    report["invalid_entity_type_examples"] = [(e.name, e.type) for e in invalid_entity_type[:10]]

    report["empty_entity_fields_count"] = len(empty_type_or_desc)
    report["empty_entity_fields_examples"] = [(e.name, e.type) for e in empty_type_or_desc[:10]]

    report["tongue_required_missing_count"] = len(tongue_required_missing)
    report["tongue_required_missing_examples"] = tongue_required_missing[:10]
    report["tongue_enum_errors_count"] = len(tongue_enum_errors)
    report["tongue_enum_errors_examples"] = tongue_enum_errors[:10]

    report["disease_missing_etiology_count"] = len(disease_missing_etiology)
    report["disease_missing_etiology_examples"] = [(e.name,) for e in disease_missing_etiology[:10]]

    report["constitution_invalid_count"] = len(constitution_invalid)
    report["constitution_invalid_examples"] = [(e.name,) for e in constitution_invalid[:10]]

    # 省略未知键统计

    report["invalid_rel_type_count"] = len(invalid_rel_type)
    report["invalid_rel_type_examples"] = [(r.source, r.rel_type, r.target) for r in invalid_rel_type[:10]]

    report["arrow_errors_count"] = len(arrow_errors)
    report["arrow_errors_examples"] = [
        (r.source, r.rel_type, r.target, why) for r, why in arrow_errors[:10]
    ]

    # Unresolved entities referenced by relationships
    unresolved = [r for r in relationships if (get_type(r.source) == "" or get_type(r.target) == "")]
    report["unresolved_relation_entities_count"] = len(unresolved)
    report["unresolved_relation_entities_examples"] = [(r.source, r.target) for r in unresolved[:10]]

    return report


def print_validation_report(report: Dict[str, Any]) -> None:
    print("\n===== 规则校验报告 =====")
    items = [
        ("不存在的实体类型", "invalid_entity_type_count", "invalid_entity_type_examples"),
        ("实体字段为空(type/description)", "empty_entity_fields_count", "empty_entity_fields_examples"),
        ("舌象缺少必填属性", "tongue_required_missing_count", "tongue_required_missing_examples"),
        ("舌象枚举值不合法(舌苔/苔润燥)", "tongue_enum_errors_count", "tongue_enum_errors_examples"),
        ("疾病缺少病因", "disease_missing_etiology_count", "disease_missing_etiology_examples"),
        ("体质名称不在九大体质", "constitution_invalid_count", "constitution_invalid_examples"),
        ("不存在的关系类型", "invalid_rel_type_count", "invalid_rel_type_examples"),
        ("关系方向/箭头不符合约定", "arrow_errors_count", "arrow_errors_examples"),
        ("关系引用了未解析实体", "unresolved_relation_entities_count", "unresolved_relation_entities_examples"),
    ]

    for title, count_key, examples_key in items:
        count = report.get(count_key, 0)
        print(f"- {title}: {count}")
        examples = report.get(examples_key) or []
        if examples:
            print(f"  示例: {examples}")
def write_test_output_csv(entities: List[EntityRecord], relationships: List[RelationshipRecord], out_path: Path) -> None:
    """Write a single CSV combining entities and relationships for quick viewing."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "record_kind",  # entity / relationship
        # entity fields
        "name", "entity_type", "description", "source_snippet", "extra_json",
        # relationship fields
        "source", "rel_type", "target", "reason", "strength", "rel_extra_json",
    ]
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        # entities
        for e in entities:
            writer.writerow({
                "record_kind": "entity",
                "name": e.name,
                "entity_type": e.type,
                "description": e.description,
                "source_snippet": e.source_snippet,
                "extra_json": json.dumps(e.extra, ensure_ascii=False),
                "source": "",
                "rel_type": "",
                "target": "",
                "reason": "",
                "strength": "",
                "rel_extra_json": "",
            })
        # relationships
        for r in relationships:
            writer.writerow({
                "record_kind": "relationship",
                "name": "",
                "entity_type": "",
                "description": "",
                "source_snippet": "",
                "extra_json": "",
                "source": r.source,
                "rel_type": r.rel_type,
                "target": r.target,
                "reason": r.reason,
                "strength": r.strength,
                "rel_extra_json": json.dumps(r.extra, ensure_ascii=False),
            })

# ---------------------- CLI ----------------------
def main():
    parser = argparse.ArgumentParser(description="Test extract_graph prompt and pretty-print results")
    parser.add_argument("--settings", default=DEFAULT_SETTINGS_PATH, help="Path to settings.yaml")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT_PATH, help="Path to extract_graph.txt")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--text", help="Raw input text")
    group.add_argument("--input-file", help="Path to an input text file")
    parser.add_argument("--max-tokens", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=None, help="Deterministic seed if backend supports it")
    args = parser.parse_args()

    # Load settings for model config
    cfg = load_settings_yaml(args.settings)
    model_cfg = (cfg.get("models", {}) or {}).get("default_chat_model", {})
    api_base = os.environ.get("OPENAI_BASE_URL") or model_cfg.get("api_base") or "http://localhost:8000/v1"
    api_key = os.environ.get("OPENAI_API_KEY") or model_cfg.get("api_key") or ""
    model = os.environ.get("OPENAI_MODEL") or model_cfg.get("model") or "gpt-3.5-turbo"

    # Entity types list
    eg_cfg = (cfg.get("extract_graph", {}) or {})
    entity_types = eg_cfg.get("entity_types") or FALLBACK_ENTITY_TYPES

    # Load prompt template
    prompt_template = read_text_file(args.prompt)

    # Input text
    if args.text:
        input_text = args.text
    else:
        input_text = read_text_file(args.input_file)

    # Build prompt
    prompt = build_prompt(prompt_template, input_text, entity_types)

    # Call model
    mdl_cfg = cfg.get("models", {}).get("default_chat_model", {})
    try:
        output_text = call_chat_completion(
            api_base=api_base,
            api_key=api_key,
            model=model,
            prompt=prompt,
            max_tokens=args.max_tokens,
            request_timeout_s=mdl_cfg.get("request_timeout", 300),
            top_p=mdl_cfg.get("top_p", 1.0),
            presence_penalty=mdl_cfg.get("presence_penalty", 0.0),
            frequency_penalty=mdl_cfg.get("frequency_penalty", 0.0),
            seed=args.seed if args.seed is not None else mdl_cfg.get("seed"),
        )
    except Exception as e:
        print(f"❌ LLM 调用失败: {e}")
        sys.exit(1)

    # Parse & print
    entities, relationships = parse_output(output_text)
    # Make output order deterministic for diffing
    entities = sorted(entities, key=lambda e: (e.type, e.name))
    # Auto-fix common arrow mistakes
    relationships, flipped = normalize_relationship_directions(entities, relationships)
    relationships = sorted(relationships, key=lambda r: (r.rel_type or "", r.source, r.target))
    pretty_print_results(entities, relationships)
    if flipped:
        print(f"\nℹ️ 已自动纠正关系方向 {len(flipped)} 条，示例: {flipped[:5]}")

    # Validation
    allowed_entity_types = [
        "症状", "疾病", "体质", "药方", "舌象", "治疗方法", "文献"
    ]
    report = validate_results(entities, relationships, allowed_entity_types)
    print_validation_report(report)

    # Raw output preview and save
    out_dir = Path("ragtest")/"output"
    preview_raw_output(output_text, save_dir=out_dir)

    # Also write a combined CSV for easy viewing
    csv_path = out_dir / "test_output.csv"
    write_test_output_csv(entities, relationships, csv_path)
    print(f"\n✅ 已写入测试输出CSV: {csv_path}")


if __name__ == "__main__":
    main()

