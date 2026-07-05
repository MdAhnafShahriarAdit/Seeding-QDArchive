"""
Part 2, Step 4d: generate the PDF report summarizing classification results.

Structure (per project description, p.30), repeated for each repository:
  1. Histogram of primary classes identified
     - full class name as the bin name
     - count labeled on top of each bar
     - vector graphics (this uses matplotlib's PDF backend, which is
       vector -- you can zoom in without pixelation)
  2. Rank-ordered list of classes in table format (top 20), with counts
  3. A comments page -- YOU must edit this with your own observations
     before submitting; the placeholder text is just a starting point.

Run from the repo root:
    python3 scripts/generate_classification_report.py [output_path]

Default output: data/sq26-classification-report.pdf
"""

import sys
import sqlite3
import textwrap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from config import DB_PATH
from isic_data import DIVISIONS

DEFAULT_OUTPUT = "data/sq26-classification-report.pdf"

REPO_NAMES = {
    7: "Repository 7 (ADA Dataverse)",
    16: "Repository 16 (uni-halle)",
}

# ---------------------------------------------------------------------------
# STYLE CONFIG -- edit these to customize colors/fonts, nothing else below
# needs to change for basic restyling.
# ---------------------------------------------------------------------------
STYLE = {
    "bar_color": "#4472C4",         # histogram bar color
    "table_header_bg": "#4472C4",   # table header row background
    "table_header_text": "white",   # table header row text color
    "table_row_alt_bg": "#F2F2F2",  # alternating table row background
    "font_family": "sans-serif",    # try "serif", "monospace", etc.
    "title_fontsize": 13,
    "label_fontsize": 8,
    "count_label_fontsize": 8,
    "comment_fontsize": 10,
}


def class_label(code, wrap_width=55):
    if not code:
        label = "UNCLASSIFIED (no confident match)"
    else:
        info = DIVISIONS.get(code)
        label = f"{code} - {info['title']}" if info else code
    return "\n".join(textwrap.wrap(label, wrap_width)) if wrap_width else label


def get_class_counts(cur, repo_id):
    cur.execute("""
        SELECT primary_class, COUNT(*) c FROM projects
        WHERE repository_id=? GROUP BY primary_class ORDER BY c DESC
    """, (repo_id,))
    return cur.fetchall()


def add_histogram_page(pdf, repo_id, repo_name, counts):
    n = len(counts)
    labels = [class_label(code) for code, _ in counts]
    max_lines = max(label.count("\n") + 1 for label in labels)
    # give each bar enough vertical room for its wrapped label
    per_bar_height = 0.28 + 0.14 * max_lines
    fig_height = max(6, per_bar_height * n + 2)
    fig, ax = plt.subplots(figsize=(12, fig_height))

    values = [c for _, c in counts]
    y_pos = range(len(labels))[::-1]  # highest count at top

    bars = ax.barh(list(y_pos), values, color=STYLE["bar_color"], height=0.65)
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(labels, fontsize=STYLE["label_fontsize"], family=STYLE["font_family"])
    ax.set_xlabel("Number of projects", family=STYLE["font_family"])
    ax.set_title(f"{repo_name}\nHistogram of primary ISIC classes identified",
                 fontsize=STYLE["title_fontsize"], fontweight="bold", family=STYLE["font_family"])

    max_val = max(values) if values else 1
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + max_val * 0.01, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", fontsize=STYLE["count_label_fontsize"],
                fontweight="bold", family=STYLE["font_family"])

    ax.set_xlim(0, max_val * 1.12)
    ax.margins(y=0.01)
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def add_table_page(pdf, repo_name, counts, top_n=20):
    top = counts[:top_n]
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    ax.set_title(f"{repo_name}\nTop {len(top)} classes, rank-ordered",
                 fontsize=STYLE["title_fontsize"], fontweight="bold", pad=20, family=STYLE["font_family"])

    table_data = [["Rank", "Class", "Count"]]
    for i, (code, count) in enumerate(top, start=1):
        table_data.append([str(i), class_label(code, wrap_width=70), str(count)])

    table = ax.table(cellText=table_data, colLabels=None, cellLoc="left",
                      colWidths=[0.08, 0.78, 0.14], loc="upper center")
    table.auto_set_font_size(False)
    table.set_fontsize(STYLE["label_fontsize"])
    table.scale(1, 1.6)

    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor(STYLE["table_header_bg"])
            cell.set_text_props(color=STYLE["table_header_text"], fontweight="bold", family=STYLE["font_family"])
        else:
            cell.set_facecolor(STYLE["table_row_alt_bg"] if row % 2 == 0 else "white")
            cell.set_text_props(family=STYLE["font_family"])

    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


FINAL_COMMENTS = {
    7: (
        'The dominant class in this repository is "Q85 - Education", accounting for 7 of 53 '
        'projects (13.2%). However, no single class dominates strongly here -- the distribution '
        'is fairly flat, with most classes represented by only 1-6 projects out of 16 distinct '
        'ISIC divisions identified.\n\n'
        'This flatness is likely a direct consequence of how this repository was acquired: all '
        'files in ADA Dataverse were marked as restricted and could not be downloaded (see '
        'TECHNICAL_CHALLENGES.md), so classification for every project here relies entirely on '
        'metadata (title, description, keywords) rather than file content. With no primary data '
        'files to classify individually, the project-level signal is thinner and more evenly '
        'spread across categories than in repo 16, where file content was available. This is a '
        "meaningful limitation to flag: repo 7's classifications should be read as "
        "lower-confidence than repo 16's."
    ),
    16: (
        'The dominant class in this repository is "N72 - Scientific research and development", '
        'accounting for 384 of 855 projects (44.9%), with "R86 - Human health activities" close '
        'behind at 341 (39.9%). Together these two classes cover roughly 85% of all projects, out '
        'of 21 distinct ISIC divisions identified.\n\n'
        'This concentration reflects two things. First, this repository is a university thesis '
        'archive, and its actual subject matter is genuinely dominated by medical and life-science '
        'research (confirmed independently by the ddc: keyword metadata, where DDC codes 610 '
        '(medicine) and 570-590 (biology) are by far the most frequent). Second, it is partly an '
        'artifact of the classifier design: ISIC has no dedicated division for basic academic '
        'disciplines like biology, physics, or mathematics, so DDC codes for these fields were '
        'mapped to the closest available catch-all, "Scientific research and development" (N72). '
        "This inflates N72's share somewhat and is worth treating as a simplification rather than "
        'a precise industry classification.\n\n'
        'A related limitation worth noting: because classification is keyword-based, it '
        'occasionally misfires on individual projects -- for example, at least one project was '
        'classified under "C16 - Manufacture of wood products," which is clearly not what a '
        'qualitative research thesis is about. This is an expected trade-off of a free, rule-based '
        'approach rather than a paid LLM-based classifier, and would be worth flagging as a '
        'direction for improvement in future iterations of this pipeline.'
    ),
}


def add_comments_page(pdf, repo_id, repo_name, counts):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    ax.set_title(f"{repo_name}\nComments on findings", fontsize=STYLE["title_fontsize"],
                 fontweight="bold", pad=20, family=STYLE["font_family"])

    raw_text = FINAL_COMMENTS.get(repo_id, "[No commentary written for this repository yet.]")

    # wrap manually (paragraph by paragraph, preserving blank lines between them)
    # rather than relying on matplotlib's approximate wrap=True, which can let
    # long words slightly overflow the axes on the right edge.
    wrapped_paragraphs = [
        "\n".join(textwrap.wrap(p, width=100)) if p else ""
        for p in raw_text.split("\n\n")
    ]
    text = "\n\n".join(wrapped_paragraphs)

    ax.text(0.02, 0.95, text, fontsize=STYLE["comment_fontsize"], va="top", ha="left",
            transform=ax.transAxes, family=STYLE["font_family"])

    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT DISTINCT repository_id FROM projects ORDER BY repository_id")
    repo_ids = [r[0] for r in cur.fetchall()]

    with PdfPages(output_path) as pdf:
        for repo_id in repo_ids:
            repo_name = REPO_NAMES.get(repo_id, f"Repository {repo_id}")
            counts = get_class_counts(cur, repo_id)
            if not counts:
                continue
            add_histogram_page(pdf, repo_id, repo_name, counts)
            add_table_page(pdf, repo_name, counts)
            add_comments_page(pdf, repo_id, repo_name, counts)

    con.close()
    print(f"Wrote report to {output_path}")
    print(f"Repositories included: {repo_ids}")


if __name__ == "__main__":
    main()
