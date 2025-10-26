# agents/simple_tagger.py
import re

class SimpleTaggerAgent:
    NAME = "Simple Tagger"
    DESCRIPTION = "Heuristically tags gender and broad culture from the logline (no prediction)."
    NEEDS_THRESHOLD = False

    def run(self, logline: str, threshold=None):
        t = (logline or "").lower()
        if   re.search(r"\b(woman|female|she|her)\b", t): g = "Female"
        elif re.search(r"\b(man|male|he|him)\b", t):      g = "Male"
        elif re.search(r"\b(non[- ]?binary|they|them)\b", t): g = "Non-binary"
        else: g = "Unknown"

        cg = "Other/Unknown"
        for kw, grp in [("american","Europe/North America"),("korean","East Asia"),("indian","South Asia"),
                        ("thai","SE Asia"),("turkish","MENA"),("nigerian","Africa"),
                        ("brazilian","Latin America"),("australian","Oceania")]:
            if kw in t: cg = grp; break

        explanation = (f"Heuristic tags only. Inferred gender={g}, culture={cg}. "
                       "No model prediction was performed.")
        trace = [
            {"step":"parse logline","result":{"gender":g,"culture":cg}},
            {"step":"generate explanation"}
        ]
        return {
            "title": "Tagging result",
            "gender_inferred": g, "culture_inferred": cg,
            "gender": g, "culture": cg,
            "explanation": explanation,
            "trace": trace
        }
