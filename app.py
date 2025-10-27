# app.py
import re
import json
import streamlit as st

st.set_page_config(page_title="Agent Hub", page_icon="🧭")
st.title("🧭 Agent Hub (text-only)")
st.caption("Choose an agent, paste a logline, and run. This is an educational demo.")

# ---- Safe import of agents registry ----
try:
    from agents import REGISTRY  # {"Agent Name": AgentClass, ...}
    if not isinstance(REGISTRY, dict) or not REGISTRY:
        raise ValueError("agents.REGISTRY is missing or empty.")
except Exception as e:
    st.error("Could not load agents. Check that `agents/__init__.py` defines REGISTRY and your imports are valid.")
    st.exception(e)
    st.stop()

# =========================
# Helpers for tags/hints
# =========================
TAG_GENDER_PAT   = re.compile(r"\s*#gender:[^\s]+", flags=re.I)
TAG_CULTURE_PAT  = re.compile(r"\s*#culture:[^\s]+", flags=re.I)
HINT_GENDER_PAT  = re.compile(r"\s*\[Lead gender:.*?\]", flags=re.I)
HINT_CULTURE_PAT = re.compile(r"\s*\[Lead culture:.*?\]", flags=re.I)

def strip_hints_and_tags(text: str) -> str:
    text = HINT_GENDER_PAT.sub("", text)
    text = HINT_CULTURE_PAT.sub("", text)
    text = TAG_GENDER_PAT.sub("", text)
    text = TAG_CULTURE_PAT.sub("", text)
    return text.strip()

def augment_with(gender: str, culture: str, base_text: str) -> str:
    """Append bracket hints + #tags for any provided gender/culture."""
    t = strip_hints_and_tags(base_text)
    if gender and gender != "Unknown":
        t += f" [Lead gender: {gender}] #gender:{gender.replace(' ','')}"
    if culture and culture not in ("Unknown", "Other/Unknown"):
        t += f" [Lead culture: {culture}] #culture:{culture.replace(' ','_').replace('/','_')}"
    return t.strip()

def verdict_with_explanation(prob: float, thr: float) -> tuple[str, str]:
    label = "Yes" if prob >= thr else "No"
    long = ("Yes: likely to be financially successful under the ROI rule used in this app."
            if label == "Yes" else
            "No: unlikely to be financially successful under the ROI rule used in this app.")
    return label, long

def render_result(result, threshold):
    st.subheader(result.get("title", "Result"))
    if all(k in result for k in ("verdict", "proba", "threshold")):
        st.markdown(
            f"**Prediction:** {result['verdict']} "
            f"(p = {result['proba']:.2f}, threshold = {result['threshold']:.2f})"
        )
        _, long = verdict_with_explanation(result["proba"], result["threshold"])
        st.caption(long)
        st.caption("Here, “success” uses an ROI idea (revenue versus budget) from the historical data. This tool is educational.")

    st.caption(
        f"Inferred → gender: **{result.get('gender','Unknown')}**, "
        f"culture: **{result.get('culture','Other/Unknown')}**"
    )

    if "explanation" in result:
        st.markdown("**Explanation**")
        st.write(result["explanation"])

    if "base_rates" in result and "n_counts" in result:
        with st.expander("Evidence used"):
            rg = result["base_rates"].get("gender"); ng = result["n_counts"].get("gender")
            rc = result["base_rates"].get("culture"); nc = result["n_counts"].get("culture")
            if rg is not None and ng is not None:
                st.markdown(f"- Gender={result.get('gender','?')}: success rate ≈ **{rg:.2f}** (n={ng})")
            if rc is not None and nc is not None:
                st.markdown(f"- Culture={result.get('culture','?')}: success rate ≈ **{rc:.2f}** (n={nc})")
            low = result.get("low_n_flags", [])
            if low:
                st.info("Limited data: " + ", ".join(low) + ". Treat as uncertain.")

    if result.get("completeness_tips"):
        with st.expander("Agent’s completeness tips"):
            for tip in result["completeness_tips"]:
                st.markdown(f"- {tip}")

    if "uncertainty_level" in result:
        st.markdown(f"_Uncertainty level: **{result['uncertainty_level']}**_")

    if "trace" in result:
        with st.expander("Show my reasoning (agent trace)"):
            for i, step in enumerate(result["trace"], start=1):
                st.markdown(f"**{i}. {step['step']}**")
                if "result" in step:
                    st.code(json.dumps(step["result"], ensure_ascii=False, indent=2))

# =========================
# Session defaults
# =========================
for k, v in {
    "logline": "",
    "await_clarify": False,
    "need_gender": False,
    "need_culture": False,
    "last_result": None,
    "override_gender": "Unknown",
    "override_culture": "Other/Unknown",
    "logline_update_pending": False,
    "logline_update_value": "",
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# =========================
# APPLY PENDING LOG LINE UPDATE (before widget!)
# =========================
if st.session_state["logline_update_pending"]:
    st.session_state["logline"] = st.session_state["logline_update_value"]
    st.session_state["logline_update_pending"] = False
    st.session_state["logline_update_value"] = ""

# =========================
# Logline input
# =========================
st.subheader("Logline")
st.caption(
    "A logline is a one-sentence summary of your film idea. Example: "
    "“An ex-paramedic must drive a witness across the city overnight while a crime boss hunts them.”"
)

if st.button("Fill with an example"):
    # This is safe because we're BEFORE the text_area creation during this rerun.
    st.session_state["logline"] = (
        "An ex-paramedic must drive a witness across the city overnight "
        "while a crime boss hunts them."
    )

st.text_area(
    "Paste your logline here:",
    key="logline",
    placeholder="One sentence about the main character, the goal, the stakes, and the obstacle.",
    height=140,
)

# =========================
# Agent picker
# =========================
agent_name = st.selectbox("Choose agent", list(REGISTRY.keys()))
AgentClass = REGISTRY.get(agent_name)

def make_agent():
    try:
        return AgentClass()
    except Exception as e:
        st.error("Failed to initialise the selected agent. Check model/CSV paths used in the agent.")
        st.exception(e)
        st.stop()

agent = make_agent()
show_thr = getattr(agent, "NEEDS_THRESHOLD", False)
threshold = st.slider("Decision threshold for 'Yes'", 0.20, 0.80, 0.45, 0.01) if show_thr else None

# =========================
# Run agent button
# =========================
if st.button("Run agent"):
    try:
        # Reset clarify mode
        st.session_state["await_clarify"] = False
        st.session_state["need_gender"] = False
        st.session_state["need_culture"] = False

        # Apply any saved overrides silently on first pass
        og = st.session_state["override_gender"]
        oc = st.session_state["override_culture"]
        first_pass_text = augment_with(og, oc, st.session_state["logline"])

        result = agent.run(first_pass_text or "", threshold)

        # Determine if we need clarification
        agent_gender  = result.get("gender", "Unknown")
        agent_culture = result.get("culture", "Other/Unknown")
        need_gender   = (agent_gender == "Unknown")
        need_culture  = (agent_culture in ("Unknown", "Other/Unknown"))

        # Store last result (for 'continue without clarifying')
        st.session_state["last_result"] = result

        if need_gender or need_culture:
            st.session_state["await_clarify"] = True
            st.session_state["need_gender"] = need_gender
            st.session_state["need_culture"] = need_culture
            st.info("I could not infer some details from the logline. You can clarify below or continue without changes.")
        else:
            # No clarify needed → just render
            render_result(result, threshold)

    except Exception as e:
        st.error("The agent raised an exception while running. Check paths and inputs.")
        st.exception(e)

# =========================
# Clarify UI (persistent until resolved)
# =========================
if st.session_state["await_clarify"]:
    with st.expander("Clarify details, apply to the text, then click Run again", expanded=True):
        col1, col2, col3 = st.columns([1, 1, 1])

        gender_choice = col1.radio(
            "Lead gender (optional):",
            ["Prefer not to say / Unknown", "Female", "Male", "Non-binary"],
            index=0, horizontal=True, key="clarify_gender_choice"
        )

        GROUPS = [
            "Europe/North America","East Asia","South Asia","Southeast Asia",
            "MENA","Africa","Latin America","Oceania","Other/Unknown"
        ]
        culture_choice = col2.selectbox(
            "Broad cultural region (optional):",
            options=GROUPS, index=8, key="clarify_culture_choice"
        )

        apply_clicked   = col1.button("Apply details to logline")
        proceed_clicked = col2.button("Continue without clarifying")
        clear_clicked   = col3.button("Clear tags from logline")

        if clear_clicked:
            # Queue a clear update and rerun BEFORE widget creation next time
            cleaned = strip_hints_and_tags(st.session_state["logline"])
            st.session_state["override_gender"] = "Unknown"
            st.session_state["override_culture"] = "Other/Unknown"
            st.session_state["logline_update_value"] = cleaned
            st.session_state["logline_update_pending"] = True
            st.session_state["await_clarify"] = False
            st.success("Cleared tags and hints from the text box.")
            st.rerun()

        if apply_clicked:
            # Save overrides and queue a text-box update (no direct write!)
            new_g = "Unknown" if "Prefer not" in gender_choice else gender_choice
            new_c = culture_choice
            st.session_state["override_gender"] = new_g
            st.session_state["override_culture"] = new_c

            augmented = augment_with(new_g, new_c, st.session_state["logline"])
            st.session_state["logline_update_value"] = augmented
            st.session_state["logline_update_pending"] = True

            # Exit clarify mode; user will click Run again
            st.session_state["await_clarify"] = False
            st.success("Added details to the text. Now click Run agent again.")
            st.rerun()

        if proceed_clicked:
            # Exit clarify mode and show the previous (first-pass) result
            st.session_state["await_clarify"] = False
            st.info("Proceeding without clarifying. Showing the current result below.")
            st.rerun()

# =========================
# Render last result if available and not in clarify mode
# =========================
if (not st.session_state["await_clarify"]) and st.session_state["last_result"] is not None:
    render_result(st.session_state["last_result"], threshold)

st.divider()
st.caption("Data: TMDb metadata for educational, non-commercial use with attribution. "
           "This is a bias-probe; not for hiring or funding decisions.")
