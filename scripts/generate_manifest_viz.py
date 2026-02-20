#!/usr/bin/env python3
"""
Generate manifest structure visualizations for Spinta documentation.

Outputs:
  docs/lt/static/manifest-struktura.png  — CSV rows color-coded as a table
  docs/lt/static/manifest-architektura.png — config.yml ↔ CSV split diagram
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import matplotlib.gridspec as gridspec

OUT_DIR = "/home/sauliuspetraitis/dsa-testing/docs/spinta_source/docs/lt/static/"

C = {
    "header_bg":   "#1D4ED8",
    "header_fg":   "white",
    "dataset":     "#DBEAFE",   # mėlynas  — vardų erdvė
    "resource":    "#FEF3C7",   # geltona  — admin nuoroda
    "separator":   "#F9FAFB",   # pilka    — tuščia eilutė
    "model":       "#D1FAE5",   # žalia    — duomenų objektas
    "prop":        "#F0FDF4",   # šviesiai žalia — laukai
    "row_label":   "#F1F5F9",
    "border":      "#CBD5E1",
    "admin_box":   "#FFF7ED",
    "admin_border":"#F59E0B",
    "admin_text":  "#92400E",
    "biz_box":     "#F0FDF4",
    "biz_border":  "#16A34A",
    "biz_text":    "#14532D",
    "arrow":       "#6B7280",
    "text":        "#111827",
    "text_light":  "#6B7280",
}

# ---------------------------------------------------------------------------
# 1. manifest-struktura.png  (CSV kaip spalvota lentelė su dviem backend'ais)
# ---------------------------------------------------------------------------

def make_struktura():
    # Row definitions:
    # (row_label, [col_values per 9 cols], bg_color, annotation_text or None)
    cols = ["dataset", "resource", "model", "property",
            "type", "ref", "source", "level", "access"]

    rows = [
        # ── 1 šaltinis: myapp_db ──────────────────────────────────────────
        ("① Dataset",
         ["datasets/gov/lt/myapp", "", "", "", "", "", "", "", ""],
         C["dataset"], None),
        ("② Resource",
         ["", "myapp_db", "", "", "sql", "myapp_db", "", "", ""],
         C["resource"],
         "backend pavadinimas iš config.yml\n(ne slaptažodis — jis tik config faile)"),
        ("(tuščia)", [""] * 9, C["separator"], None),
        ("③ Model",
         ["", "", "Country", "", "", "id", "country", "", ""],
         C["model"], None),
        ("④ Property",
         ["", "", "", "id", "integer", "", "id", "develop", "private"],
         C["prop"], None),
        ("④ Property",
         ["", "", "", "name", "string", "", "name", "develop", "private"],
         C["prop"], None),

        # ── tarpas tarp šaltinių ──────────────────────────────────────────
        ("", [""] * 9, "#FFFFFF", None),

        # ── 2 šaltinis: products_db ───────────────────────────────────────
        ("① Dataset",
         ["datasets/gov/lt/products", "", "", "", "", "", "", "", ""],
         C["dataset"], None),
        ("② Resource",
         ["", "products_db", "", "", "sql", "products_db", "", "", ""],
         C["resource"],
         "kitas backend — kita DB,\ntie patys CSV faile"),
        ("(tuščia)", [""] * 9, C["separator"], None),
        ("③ Model",
         ["", "", "Product", "", "", "id", "product", "", ""],
         C["model"], None),
        ("④ Property",
         ["", "", "", "id", "integer", "", "id", "develop", "private"],
         C["prop"], None),
        ("④ Property",
         ["", "", "", "name", "string", "", "name", "develop", "private"],
         C["prop"], None),
        ("④ Property",
         ["", "", "", "price", "number", "", "price", "develop", "private"],
         C["prop"], None),
    ]

    n_cols = len(cols)
    # Layout: table starts at x=0.17 (leaving room for left labels)
    table_left = 0.18
    table_w = 0.80          # table width in axes fraction
    cell_w = table_w / n_cols
    cell_h = 0.063
    start_y = 0.95

    fig = plt.figure(figsize=(17, 10), facecolor="white")
    gs = gridspec.GridSpec(1, 2, figure=fig, width_ratios=[3.2, 1], wspace=0.03)

    ax = fig.add_subplot(gs[0])
    ax.set_facecolor("white")
    ax.axis("off")

    # ── Column headers ──
    for j, c in enumerate(cols):
        x = table_left + j * cell_w
        rect = FancyBboxPatch((x + 0.002, start_y), cell_w - 0.004, cell_h,
                              boxstyle="square,pad=0", linewidth=0.5,
                              edgecolor=C["border"], facecolor=C["header_bg"],
                              transform=ax.transAxes, clip_on=False)
        ax.add_patch(rect)
        ax.text(x + cell_w / 2, start_y + cell_h / 2, c,
                ha="center", va="center", fontsize=8, fontweight="bold",
                color=C["header_fg"], transform=ax.transAxes)

    # ── Data rows ──
    annotation_targets = []  # (axes_x, axes_y, text) for annotations
    for i, (label, vals, color, annot) in enumerate(rows):
        y = start_y - (i + 1) * cell_h
        y_mid = y + cell_h / 2

        # Row label (left of table)
        if label:
            is_resource = label == "② Resource"
            ax.text(table_left - 0.01, y_mid, label,
                    ha="right", va="center", fontsize=7,
                    color=C["admin_text"] if is_resource else C["text"],
                    fontweight="bold" if label not in ("(tuščia)", "") else "normal",
                    transform=ax.transAxes)

        # Cells
        for j, val in enumerate(vals):
            x = table_left + j * cell_w
            rect = FancyBboxPatch((x + 0.002, y), cell_w - 0.004, cell_h - 0.003,
                                  boxstyle="square,pad=0", linewidth=0.3,
                                  edgecolor=C["border"], facecolor=color,
                                  transform=ax.transAxes, clip_on=False)
            ax.add_patch(rect)
            # Truncate long values
            display = val if len(val) <= 22 else val[:20] + "…"
            ax.text(x + cell_w / 2, y_mid, display,
                    ha="center", va="center", fontsize=7.5,
                    color=C["text"], transform=ax.transAxes)

        if annot:
            annotation_targets.append((table_left + cell_w * 1.5, y_mid, annot, i))

    # ── Annotations for Resource rows ──
    offsets = [(-0.09, -0.10), (-0.09, -0.08)]
    for idx, (ax_x, ax_y, txt, row_i) in enumerate(annotation_targets):
        ox, oy = offsets[idx]
        ax.annotate(
            txt,
            xy=(ax_x, ax_y), xycoords="axes fraction",
            xytext=(ax_x + ox, ax_y + oy), textcoords="axes fraction",
            fontsize=7.5, color=C["admin_text"],
            arrowprops=dict(arrowstyle="->", color=C["admin_text"], lw=1.1),
            bbox=dict(boxstyle="round,pad=0.3", facecolor=C["admin_box"],
                      edgecolor=C["admin_border"], linewidth=1.1),
        )

    ax.set_title("manifest.csv — vienas failas, du šaltiniai, du backend'ai",
                 fontsize=12, fontweight="bold", pad=12, loc="left")
    ax.text(table_left, 0.02,
            "Rodomi pagrindiniai stulpeliai. Visas DSA 1.1 sąrašas: id, dataset, resource, base, model, "
            "property, type, ref, source, source.type, prepare, origin, count, level, status, "
            "visibility, access, uri, eli, title, description",
            ha="left", va="top", fontsize=6.5, color=C["text_light"],
            transform=ax.transAxes, wrap=True)

    # ── Legend ──
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor("white")
    ax2.axis("off")

    legend_items = [
        (C["dataset"],  "① Dataset\n(vardų erdvė / org.)"),
        (C["resource"], "② Resource\n(šaltinio tipas +\nbackend nuoroda)"),
        (C["model"],    "③ Model\n(duomenų objektas)"),
        (C["prop"],     "④ Property\n(laukas / stulpelis)"),
    ]
    ax2.text(0.05, 0.98, "Eilučių tipai", fontsize=10, fontweight="bold",
             va="top", transform=ax2.transAxes)

    for idx, (color, desc) in enumerate(legend_items):
        y = 0.88 - idx * 0.20
        rect = FancyBboxPatch((0.05, y - 0.065), 0.16, 0.10,
                              boxstyle="round,pad=0.01", linewidth=1,
                              edgecolor=C["border"], facecolor=color,
                              transform=ax2.transAxes, clip_on=False)
        ax2.add_patch(rect)
        ax2.text(0.26, y - 0.01, desc, va="center", ha="left",
                 fontsize=8, color=C["text"], transform=ax2.transAxes)

    # Dividing line between two sources (visual guide)
    ax2.text(0.05, 0.12,
             "Viename CSV gali buti\nkeli šaltiniai su skirtingais\nbackend'ais.\n\n"
             "Backend'ų skaičius —\nnėra apribojimų.",
             ha="left", va="top", fontsize=8, color=C["biz_text"],
             transform=ax2.transAxes,
             bbox=dict(boxstyle="round,pad=0.4", facecolor=C["biz_box"],
                       edgecolor=C["biz_border"], linewidth=1))

    admin_patch = mpatches.Patch(facecolor=C["admin_box"], edgecolor=C["admin_border"],
                                 linewidth=1.5, label="Admin (config.yml)")
    ax2.legend(handles=[admin_patch], loc="lower center",
               bbox_to_anchor=(0.5, 0.01), fontsize=8, frameon=True,
               title="Atskiras failas:", title_fontsize=7.5)

    plt.savefig(OUT_DIR + "manifest-struktura.png",
                dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print("✓ manifest-struktura.png")


# ---------------------------------------------------------------------------
# 2. manifest-architektura.png  (config.yml ↔ sDSA failai, 3 backend'ai)
# ---------------------------------------------------------------------------

def make_architektura():
    fig, ax = plt.subplots(figsize=(17, 9.5), facecolor="white")
    ax.set_xlim(0, 17)
    ax.set_ylim(0, 9.5)
    ax.axis("off")

    def rbox(x, y, w, h, bg, border, lw=1.8):
        r = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.12",
                           linewidth=lw, edgecolor=border, facecolor=bg)
        ax.add_patch(r)

    def ttext(x, y, w, h, text, color, fs=10.5):
        ax.text(x + w / 2, y + h - 0.3, text,
                ha="center", va="top", fontsize=fs, fontweight="bold", color=color)

    def mlines(x, y, lines, fs=8.8, color="#111827"):
        for i, ln in enumerate(lines):
            ax.text(x, y - i * 0.40, ln,
                    ha="left", va="top", fontsize=fs, color=color, family="monospace")

    # ── config.yml (admin, kairė) ──
    rbox(0.3, 0.8, 5.8, 8.3, C["admin_box"], C["admin_border"])
    ttext(0.3, 0.8, 5.8, 8.3, "config.yml  [tik admin]", C["admin_text"], fs=11.5)
    mlines(0.55, 8.55, [
        "backends:",
        "",
        "  myapp_db:",
        "    type: sql",
        "    dsn: postgresql://admin:",
        "         secret1@host1/myapp",
        "",
        "  products_db:",
        "    type: sql",
        "    dsn: postgresql://user:",
        "         secret2@host2/products",
        "",
        "  archive_db:",
        "    type: sql",
        "    dsn: postgresql://ro_user:",
        "         secret3@host3/archive",
        "",
        "manifests:",
        "  default:",
        "    path: /opt/spinta/manifest.csv",
    ], fs=8.8, color=C["admin_text"])
    ax.text(0.3 + 5.8 / 2, 0.62,
            "Slaptazodziai — TIK cia. Mato tik administratorius.",
            ha="center", va="top", fontsize=9, color=C["admin_text"], style="italic")

    # ── manifest.csv ──
    rbox(6.9, 0.8, 5.8, 8.3, C["biz_box"], C["biz_border"])
    ttext(6.9, 0.8, 5.8, 8.3, "manifest.csv", C["biz_text"], fs=11.5)
    mlines(7.15, 8.55, [
        "datasets/gov/lt/myapp",
        "  resource: myapp_db  <──────────",
        "    Country",
        "      id     integer",
        "      name   string",
        "",
        "datasets/gov/lt/products",
        "  resource: products_db  <───────",
        "    Product",
        "      id     integer",
        "      name   string",
        "      price  number",
        "",
        "datasets/gov/lt/archive",
        "  resource: archive_db  <────────",
        "    Document",
        "      id     integer",
        "      title  string",
    ], fs=8.8, color=C["text"])
    ax.text(6.9 + 5.8 / 2, 0.62,
            "DSA struktura — gali matyti veiklos zmones. Jokiu slaptazodziu.",
            ha="center", va="top", fontsize=9, color=C["biz_text"], style="italic")

    # ── Rodyklės (resource → backend) ──
    arrow_y = [(7.70, 7.40), (5.70, 5.15), (3.70, 2.90)]  # config.yml y, manifest y
    labels   = ["myapp_db", "products_db", "archive_db"]
    for (cy, my), lbl in zip(arrow_y, labels):
        ax.annotate("", xy=(6.72, my), xytext=(6.1, cy),
                    arrowprops=dict(arrowstyle="<-", color=C["admin_border"],
                                    lw=2.2, connectionstyle="arc3,rad=0.0"))
        ax.text(6.38, (cy + my) / 2 + 0.1, lbl, ha="center", fontsize=8.5,
                color=C["admin_text"], fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                          edgecolor=C["admin_border"], linewidth=0.8))

    # ── API endpoints (dešinė) ──
    rbox(13.3, 4.2, 3.4, 4.9, "#EFF6FF", "#93C5FD")
    ttext(13.3, 4.2, 3.4, 4.9, "API endpoints", "#1D4ED8", fs=10.5)
    for i, path in enumerate([
        "/...myapp/Country",
        "/...products/Product",
        "/...archive/Document",
    ]):
        y = 8.45 - i * 1.2
        ax.text(13.55, y, path, ha="left", fontsize=8.5,
                color="#1D4ED8", family="monospace")

    # Rodyklės iš manifest.csv → endpoints
    for src_y, dst_y in [(7.50, 8.10), (5.15, 6.90), (2.90, 5.70)]:
        ax.annotate("", xy=(13.3, dst_y), xytext=(12.7, src_y),
                    arrowprops=dict(arrowstyle="->", color=C["biz_border"], lw=1.5,
                                    connectionstyle="arc3,rad=-0.2"))

    ax.set_title(
        "config.yml ir manifest.csv: slaptazodziai atskirti nuo DSA strukturos "
        "(backend'u skaicius — neribojamas)",
        fontsize=12, fontweight="bold", pad=14)

    plt.tight_layout()
    plt.savefig(OUT_DIR + "manifest-architektura.png",
                dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print("✓ manifest-architektura.png")


if __name__ == "__main__":
    make_struktura()
    make_architektura()
    print("Done.")
