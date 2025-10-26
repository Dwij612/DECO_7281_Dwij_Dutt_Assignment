# agents/bias_auditor.py
import re, json
import pandas as pd
from agents._resources import get_resources

# Optional OpenAI for nicer phrasing (falls back if missing)
try:
    from openai import OpenAI
    _OPENAI = OpenAI()   # needs OPENAI_API_KEY
except Exception:
    _OPENAI = None

def _canonicalize_culture(val: str) -> str:
    s = (val or "").strip().lower().replace("-", " ")
    s = " ".join(s.split())
    def has(*xs): return any(x in s for x in xs)
    if has("american","usa","canadian","british","france","german","italian","spanish","europe","north america","mexic"): return "Europe/North America"
    if has("korea","japan","china","taiwan","hong kong","east asia"): return "East Asia"
    if has("india","pakistan","bangladesh","sri lanka","nepal","bhutan","south asia"): return "South Asia"
    if has("thailand","vietnam","indonesia","malaysia","philipp","singapore","se asia","southeast"): return "SE Asia"
    if has("turk","saudi","iran","iraq","lebanon","egypt","morocc","mena","middle east","north africa"): return "MENA"
    if has("brazil","argentin","chile","colomb","peru","venez","latam","latin america","latino","latina","latinx"): return "Latin America"
    if has("niger","ghana","kenya","south africa","ethiop","tanzan","uganda","africa"): return "Africa"
    if has("australia","new zealand","kiwi","oceania","papua","fiji"): return "Oceania"
    return "Other/Unknown"

def _extract_from_logline(text: str):
    t = (text or "").lower()
    if re.search(r"\b(woman|female|she|her)\b", t): lg = "Female"
    elif re.search(r"\b(man|male|he|him)\b", t):     lg = "Male"
    elif re.search(r"\b(non[- ]?binary|they|them)\b", t): lg = "Non-binary"
    else: lg = "Unknown"
    cg = "Other/Unknown"
    for kw, grp in [
        ("american", "Europe/North America"), ("usa", "Europe/North America"), ("british", "Europe/North America"),
        ("korean", "East Asia"), ("japanese", "East Asia"), ("chinese", "East Asia"),
        ("indian", "South Asia"), ("pakistani", "South Asia"), ("bangladeshi", "South Asia"),
        ("thai", "SE Asia"), ("vietnamese", "SE Asia"), ("indonesian", "SE Asia"), ("malaysian", "SE Asia"),
        ("turkish", "MENA"), ("iranian", "MENA"), ("egyptian", "MENA"),
        ("nigerian", "Africa"), ("kenyan", "Africa"), ("south african", "Africa"),
        ("brazilian", "Latin America"), ("argentine", "Latin America"), ("chilean", "Latin America"),
        ("australian", "Oceania"), ("new zealander", "Oceania"), ("kiwi", "Oceania"),
    ]:
        if kw in t: cg = grp; break
    return {"lead_gender": lg, "lead_culture_group": cg}

def _assess_uncertainty(proba: float, threshold: float, n_g: int, n_c: int, min_n: int):
    margin = abs(proba - threshold)
    low_data = (n_g < min_n) or (n_c < min_n)
    if margin >= 0.20 and not low_data:
        return "High", "Probability far from threshold with adequate samples."
    if margin >= 0.10 and not low_data:
        return "Medium", "Moderate distance from threshold with adequate samples."
    if low_data:
        return "Low", "Limited data and/or close to threshold."
    return "Low", "Probability is close to the threshold."

def _explain(verdict, proba, thr, g, cg, r_g, n_g, r_c, n_c, low_n, level, reason):
    base = (f"Decision: {verdict} (p={proba:.2f}, threshold={thr:.2f}). "
            f"Base rates: gender={g} → {r_g:.2f} (n={n_g}); culture={cg} → {r_c:.2f} (n={n_c}). "
            f"Uncertainty: {level}. {reason}")
    if low_n:
        base += " Limited data for " + ", ".join(low_n) + "."
    if not _OPENAI:
        return base
    try:
        r = _OPENAI.responses.create(
            model="gpt-4.1-mini",
            input=[{"role":"user","content":"Rewrite in 2–3 plain sentences, keep all numbers:\n"+base}]
        )
        return r.output_text.strip()
    except Exception:
        return base

class BiasAuditorAgent:
    NAME = "Bias Auditor"
    DESCRIPTION = "Audits a logline using a demographics-only model with evidence and a reasoning trace."
    NEEDS_THRESHOLD = True

    def __init__(self):
        res = get_resources()
        self.pipe = res["pipe"]
        self.df_base = res["df_base"]
        self.sr_gender = res["sr_gender"]
        self.sr_cult = res["sr_cult"]
        self.global_sr = res["global_sr"]
        self.MIN_N = res["MIN_N"]
        self.GENDERS = res["GENDERS"]

    def run(self, logline: str, threshold: float | None):
        if threshold is None:
            threshold = 0.45

        trace = []
        plan = ["parse logline", "canonicalise", "check completeness",
                "predict", "collect evidence", "assess uncertainty",
                "generate explanation", "policy guard"]
        trace.append({"step":"plan", "result":{"sequence":plan}})

        parsed = _extract_from_logline(logline)
        trace.append({"step":"parse logline", "result": parsed})

        g_raw  = parsed.get("lead_gender","Unknown")
        cg_raw = parsed.get("lead_culture_group","Other/Unknown")
        g  = g_raw if g_raw in self.GENDERS else "Unknown"
        cg = _canonicalize_culture(cg_raw)
        trace.append({"step":"canonicalise", "result":{"lead_gender": g, "lead_culture_group": cg}})

        tips = []
        if g == "Unknown": tips.append("Consider stating the lead’s gender.")
        if cg == "Other/Unknown": tips.append("Consider stating a nationality or cultural cue.")
        trace.append({"step":"check completeness", "result":{"needs_clarification": bool(tips), "tips": tips}})

        X = pd.DataFrame([{"lead_gender": g, "lead_culture_group": cg}])
        proba = float(self.pipe.predict_proba(X)[0][1])
        verdict = "Yes" if proba >= threshold else "No"
        trace.append({"step":"predict", "result":{"proba": proba, "threshold": threshold, "verdict": verdict}})

        r_g, n_g = self.sr_gender.get(g, (self.global_sr, len(self.df_base)))
        r_c, n_c = self.sr_cult.get(cg, (self.global_sr, len(self.df_base)))
        low_n = []
        if n_g < self.MIN_N: low_n.append(f"gender={g} (n={n_g})")
        if n_c < self.MIN_N: low_n.append(f"culture={cg} (n={n_c})")
        trace.append({"step":"collect evidence", "result":{
            "gender":{"base_rate": r_g, "n": n_g},
            "culture":{"base_rate": r_c, "n": n_c},
            "low_n": low_n
        }})

        level, reason = _assess_uncertainty(proba, threshold, n_g, n_c, self.MIN_N)[:2]
        trace.append({"step":"assess uncertainty", "result":{"level": level, "reason": reason}})

        explanation = _explain(verdict, proba, threshold, g, cg, r_g, n_g, r_c, n_c, low_n, level, reason)
        trace.append({"step":"generate explanation"})

        policy = "Educational bias probe; not for greenlighting, hiring, or funding."
        trace.append({"step":"policy guard", "result":{"message": policy}})

        return {
            "title": "Bias audit result",
            "verdict": verdict,
            "proba": proba,
            "threshold": threshold,
            "gender_inferred": g_raw, "culture_inferred": cg_raw,
            "gender": g, "culture": cg,
            "base_rates": {"gender": r_g, "culture": r_c},
            "n_counts": {"gender": n_g, "culture": n_c},
            "low_n_flags": low_n,
            "uncertainty_level": level,
            "explanation": explanation,
            "completeness_tips": tips,
            "trace": trace
        }
