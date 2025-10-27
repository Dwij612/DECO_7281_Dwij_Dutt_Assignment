# agents/clarify_agent.py
import streamlit as st

GROUPS = [
    "Europe/North America", "East Asia", "South Asia", "Southeast Asia",
    "MENA", "Africa", "Latin America", "Oceania", "Other/Unknown"
]

def run(lead_gender: str, lead_culture_group: str) -> tuple[str, str, bool]:
    """
    Ask the user to clarify missing identity fields only if needed.
    Returns (lead_gender, lead_culture_group, proceed).
    """
    need_gender = (lead_gender == "Unknown")
    need_culture = (lead_culture_group in ("Unknown", "Other/Unknown"))

    if not (need_gender or need_culture):
        return lead_gender, lead_culture_group, True

    st.info("I could not infer some details from the logline. You can clarify below or continue without changes.")

    if need_gender:
        g_choice = st.radio(
            "Lead gender (optional):",
            ["Prefer not to say / Unknown", "Female", "Male", "Non-binary"],
            index=0, horizontal=True
        )
        lead_gender = "Unknown" if "Prefer not" in g_choice else g_choice

    if need_culture:
        c_choice = st.selectbox(
            "Broad cultural region of the lead (optional):",
            options=GROUPS,
            index=8  # Other/Unknown
        )
        lead_culture_group = c_choice

    proceed = st.checkbox("Continue without clarifying")
    return lead_gender, lead_culture_group, proceed
