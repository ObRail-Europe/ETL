"""
Design system ObRail — palette, template Plotly global, helpers UI.

Encodage strict (appliqué à tous les graphiques) :
  NIGHT_BLUE  → trains de nuit, éléments institutionnels
  AMBER       → trains de jour
  GREEN_CO2   → émissions CO₂, durabilité
  SLATE       → neutres, données secondaires
  DANGER_RED  → alertes, erreurs, dépassements
"""

import plotly.io as pio
import plotly.graph_objects as go

# Palette canonique partagée entre UI et graphiques pour garder des codes visuels constants.
COLORS = {
    "night_blue":  "#393360",
    "night_deep":  "#2A2548",
    "amber":       "#EED679",
    "amber_dark":  "#D4BC5E",
    "amber_light": "#F5E49E",
    "green_co2":   "#2D7A5F",
    "slate":       "#4A6A8A",
    "night_light": "#5B5488",
    "beige_bg":    "#F5F0E8",
    "beige_warm":  "#EDE7DB",
    "card_bg":     "#FFFFFF",
    "plot_bg":     "#FAFAF5",
    "text_main":   "#1A2332",
    "text_muted":  "#6B7C8D",
    "danger":      "#C0392B",
    "border":      "#D5CFC6",
    "border_light": "#E3DED5",
}

# Séquence pensée pour distinguer les séries dès le premier regard, même en petit format.
COLOR_SEQUENCE = [
    COLORS["night_blue"],
    COLORS["amber"],
    COLORS["green_co2"],
    COLORS["night_light"],
    COLORS["slate"],
    "#8E5EA2",  # Accent complémentaire pour séries secondaires.
    "#C0392B",  # Réserve visuelle pour alertes/comparaisons défavorables.
    "#1A9C85",  # Teinte de respiration pour éviter des palettes trop monotones.
]

# Template global: évite de répéter la même configuration sur chaque figure.
_layout = go.Layout(
    font=dict(family="Inter, 'Segoe UI', system-ui, sans-serif", size=13, color=COLORS["text_main"]),
    paper_bgcolor=COLORS["card_bg"],
    plot_bgcolor=COLORS["plot_bg"],
    colorway=COLOR_SEQUENCE,
    margin=dict(l=48, r=16, t=48, b=40),
    xaxis=dict(
        gridcolor=COLORS["border_light"],
        linecolor=COLORS["border"],
        zerolinecolor=COLORS["border"],
        tickcolor=COLORS["text_muted"],
        title_font=dict(size=12, color=COLORS["text_muted"]),
    ),
    yaxis=dict(
        gridcolor=COLORS["border_light"],
        linecolor=COLORS["border"],
        zerolinecolor=COLORS["border"],
        tickcolor=COLORS["text_muted"],
        title_font=dict(size=12, color=COLORS["text_muted"]),
    ),
    legend=dict(
        bgcolor="rgba(255,255,255,0.9)",
        bordercolor=COLORS["border"],
        borderwidth=1,
        font=dict(size=12, color=COLORS["text_main"]),
    ),
    hoverlabel=dict(
        bgcolor=COLORS["text_main"],
        bordercolor=COLORS["amber"],
        font=dict(color="#FFFFFF", size=12),
    ),
    title_font=dict(size=15, color=COLORS["text_main"]),
)

pio.templates["obrail"] = go.layout.Template(layout=_layout)
pio.templates.default = "obrail"

PLOTLY_TEMPLATE = "obrail"

# Helpers de mapping métier -> couleur afin d'éviter les incohérences entre pages.
def mode_color(mode: str) -> str:
    """Retourne la couleur canonique pour un mode de transport."""
    mapping = {
        "night": COLORS["night_blue"],
        "night_train": COLORS["night_blue"],
        "day":   COLORS["amber"],
        "train": COLORS["amber"],
        "flight":COLORS["slate"],
        "co2":   COLORS["green_co2"],
    }
    return mapping.get(str(mode).lower(), COLORS["slate"])


# Helpers de formatage pour afficher des valeurs homogènes partout dans le dashboard.
def fmt_number(value, decimals: int = 0, suffix: str = "") -> str:
    """Formate un nombre avec séparateurs de milliers et suffixe optionnel."""
    try:
        v = float(value)
        formatted = f"{v:,.{decimals}f}".replace(",", "\u202f")  # Espace fine pour une lecture plus propre des milliers.
        return f"{formatted}{suffix}"
    except (TypeError, ValueError):
        return "N/A"


def fmt_pct(value, decimals: int = 1) -> str:
    return fmt_number(value, decimals, "\u202f%")


def fmt_km(value) -> str:
    return fmt_number(value, 0, "\u202fkm")


def fmt_co2(value) -> str:
    return fmt_number(value, 1, "\u202fgCO₂")


def fmt_co2_adaptive(value) -> str:
    """Formate CO2 : kgCO2eq si > 1000g, sinon gCO2eq."""
    try:
        v = float(value)
        if v > 1000:
            return fmt_number(v / 1000, 1, "\u202fkgCO₂")
        else:
            return fmt_number(v, 0, "\u202fgCO₂")
    except (TypeError, ValueError):
        return "N/A"


def fmt_duration(minutes) -> str:
    """Convertit des minutes en affichage Xh Ymin."""
    try:
        m = int(minutes)
        return f"{m // 60}h{m % 60:02d}"
    except (TypeError, ValueError):
        return "N/A"
