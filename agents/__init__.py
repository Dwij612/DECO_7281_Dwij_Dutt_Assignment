# agents/__init__.py
from .bias_auditor import BiasAuditorAgent
from .simple_tagger import SimpleTaggerAgent

REGISTRY = {
    "Bias Auditor": BiasAuditorAgent,     # uses your trained model
    "Simple Tagger (no model)": SimpleTaggerAgent,
}
