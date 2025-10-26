# app.py
import json
import streamlit as st
from agents import REGISTRY  # {"Agent Name": AgentClass, ...}

st.set_page_config(page_title="Agent Hub", page_icon="🧭")
st.title("🧭 Agent Hub (text-only)")
st.caption("Choose an agent, paste a logline, and run. This is an educational demo.")

# Pick an agent
agent_name = st.selectbox("Choose agent", list(REGISTRY.keys()))
AgentClass = REGISTRY[agent_name]
agent = AgentClass()

# Inputs
logline = st.text_area("Paste your logline", height=140,
                       placeholder="e.g., A Turkish woman paramedic drives a witness across Istanbul overnight...")
show_thr = getattr(agent, "NEEDS_THRESHOLD", False)
threshold = st.slider("Decision threshold for 'Yes'", 0.20, 0.80, 0.45, 0.01) if show_thr else None

if st.button("Run agent"):
    result = agent.run(logline or "", threshold)

    # Generic renderer (works for both agents)
    title = result.get("title", "Result")
    st.subheader(title)

    # Verdict/probability (if provided by the agent)
    if "verdict" in result and "proba" in result and "threshold" in result:
        st.markdown(f"**Prediction:** {result['verdict']}  "
                    f"(p = {result['proba']:.2f}, threshold = {result['threshold']:.2f})")

    # Inference recap (if provided)
    if "gender_inferred" in result or "culture_inferred" in result:
        st.caption(
            f"Inferred → gender: **{result.get('gender_inferred','')}** → **{result.get('gender','')}**, "
            f"culture: **{result.get('culture_inferred','')}** → **{result.get('culture','')}**"
        )

    # Explanation
    if "explanation" in result:
        st.markdown("**Explanation**")
        st.write(result["explanation"])

    # Evidence (if provided)
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

    # Completeness tips (optional)
    if result.get("completeness_tips"):
        with st.expander("Agent’s completeness tips"):
            for tip in result["completeness_tips"]:
                st.markdown(f"- {tip}")

    # Uncertainty level (optional)
    if "uncertainty_level" in result:
        st.markdown(f"_Uncertainty level: **{result['uncertainty_level']}**_")

    # Agent trace
    if "trace" in result:
        with st.expander("Show my reasoning (agent trace)"):
            for i, step in enumerate(result["trace"], start=1):
                st.markdown(f"**{i}. {step['step']}**")
                if "result" in step:
                    st.code(json.dumps(step["result"], ensure_ascii=False, indent=2))

st.divider()
st.caption("Data: TMDb metadata for educational, non-commercial use with attribution. "
           "This is a bias-probe; not for hiring or funding decisions.")
