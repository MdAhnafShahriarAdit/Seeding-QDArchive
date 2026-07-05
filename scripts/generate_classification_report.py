"""
Part 2, Step 4d: generate the PDF report summarizing classification results.

Produces a properly structured report:
  1. Cover page (course title, name, ID, semester, submitted to)
  2. Table of contents
  3. Per repository, in order:
     a. Histogram of primary classes (vector graphics, counts on bars)
     b. Rank-ordered table of top 20 classes
     c. Comments on findings (final written text, see FINAL_COMMENTS below)

Every page (except the cover) has a running header and a page number footer.
All pages are the same A4 size -- no more mismatched page dimensions.

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
# COVER PAGE INFO -- edit these if anything changes
# ---------------------------------------------------------------------------
COVER = {
    "course_title": "Seeding QDArchive",
    "subtitle": "Part 2: Classification -- Results Report",
    "name": "MD AHNAF SHAHRIAR",
    "student_id": "23206422",
    "semester": "Winter 2025 / 26 + Summer 2026",
    "submitted_to": "Dirk Riehle, FAU Erlangen",
}

# ---------------------------------------------------------------------------
# STYLE CONFIG -- edit these to customize colors/fonts
# ---------------------------------------------------------------------------
STYLE = {
    "bar_color": "#4472C4",
    "table_header_bg": "#4472C4",
    "table_header_text": "white",
    "table_row_alt_bg": "#F2F2F2",
    "font_family": "sans-serif",
    "title_fontsize": 13,
    "label_fontsize": 8,
    "count_label_fontsize": 8,
    "comment_fontsize": 10,
}

# A4 portrait, in inches
PAGE_SIZE = (8.27, 11.69)
REPORT_HEADER_TEXT = "Seeding QDArchive -- Part 2 Classification Report"

# Layout constants -- shared by every content page so headers/footers and
# content boundaries line up consistently across the whole report.
HEADER_TOP_Y = 0.965        # small running header line (report title)
HEADER_TITLE_Y = 0.935      # bold section title line
HEADER_RULE_Y = 0.905       # divider rule under the header
CONTENT_TOP = 0.85          # content area starts here, well clear of the rule
CONTENT_BOTTOM = 0.09       # content area ends here, clear of the footer
FOOTER_RULE_Y = 0.06
FOOTER_TEXT_Y = 0.04

FINAL_COMMENTS = {
    7: (
        'The dominant class in this repository is "Q85 - Education", accounting for 7 of 53 '
        'projects (13.2%). However, no single class dominates strongly here. The distribution '
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
        'occasionally misfires on individual projects, for example, at least one project was '
        'classified under "C16 - Manufacture of wood products," which is clearly not what a '
        'qualitative research thesis is about. This is an expected trade-off of a free, rule-based '
        'approach rather than a paid LLM-based classifier, and would be worth flagging as a '
        'direction for improvement in future iterations of this pipeline.'
    ),
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


def add_header_footer(fig, header_title, page_num, total_pages):
    """Adds a running header and 'Page N of Total' footer to a content page."""
    fig.text(0.5, HEADER_TOP_Y, REPORT_HEADER_TEXT, ha="center", va="center",
              fontsize=8.5, color="#666666", family=STYLE["font_family"])
    fig.text(0.5, HEADER_TITLE_Y, header_title, ha="center", va="center",
              fontsize=12, fontweight="bold", family=STYLE["font_family"])
    fig.add_artist(plt.Line2D([0.08, 0.92], [HEADER_RULE_Y, HEADER_RULE_Y],
                               color="#B0B0B0", linewidth=1.0))

    fig.add_artist(plt.Line2D([0.08, 0.92], [FOOTER_RULE_Y, FOOTER_RULE_Y],
                               color="#DDDDDD", linewidth=0.8))
    fig.text(0.5, FOOTER_TEXT_Y, f"Page {page_num} of {total_pages}", ha="center", va="center",
              fontsize=8.5, color="#666666", family=STYLE["font_family"])


def add_cover_page(pdf):
    fig = plt.figure(figsize=PAGE_SIZE)
    fig.patch.set_facecolor("white")

    fig.text(0.5, 0.72, COVER["course_title"], ha="center", va="center",
              fontsize=26, fontweight="bold", family=STYLE["font_family"])
    fig.text(0.5, 0.66, COVER["subtitle"], ha="center", va="center",
              fontsize=15, family=STYLE["font_family"])

    fig.add_artist(plt.Line2D([0.25, 0.75], [0.60, 0.60], color="#4472C4", linewidth=1.5))

    info_lines = [
        ("Name", COVER["name"]),
        ("Student ID", COVER["student_id"]),
        ("Semester", COVER["semester"]),
        ("Submitted to", COVER["submitted_to"]),
    ]
    y = 0.50
    for label, value in info_lines:
        fig.text(0.5, y, f"{label}:  {value}", ha="center", va="center",
                  fontsize=12, family=STYLE["font_family"])
        y -= 0.045

    pdf.savefig(fig)
    plt.close(fig)


def add_toc_page(pdf, entries, total_pages):
    fig = plt.figure(figsize=PAGE_SIZE)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")

    add_header_footer(fig, "Table of Contents", 2, total_pages)

    y = CONTENT_TOP
    for title, page_num, indent in entries:
        x = 0.12 + indent * 0.04
        fig.text(x, y, title, ha="left", va="center", fontsize=11 if indent == 0 else 10,
                  fontweight="bold" if indent == 0 else "normal", family=STYLE["font_family"])
        fig.text(0.88, y, str(page_num), ha="right", va="center", fontsize=10,
                  family=STYLE["font_family"])
        # dotted leader line
        fig.add_artist(plt.Line2D([x + 0.32, 0.86], [y, y], color="#DDDDDD",
                                   linewidth=0.6, linestyle=(0, (1, 2))))
        y -= 0.05 if indent == 0 else 0.045

    pdf.savefig(fig)
    plt.close(fig)


def add_histogram_page(pdf, repo_name, counts, page_num, total_pages):
    fig = plt.figure(figsize=PAGE_SIZE)
    # Wider left margin so long wrapped class names have room and never
    # get clipped off the page edge.
    ax = fig.add_axes([0.42, CONTENT_BOTTOM, 0.50, CONTENT_TOP - CONTENT_BOTTOM])

    n = len(counts)
    labels = [class_label(code, wrap_width=38) for code, _ in counts]
    values = [c for _, c in counts]
    y_pos = list(range(n))[::-1]

    # scale font/bar sizing down as bar count grows, so everything fits on
    # one fixed A4 page regardless of how many distinct classes were found
    label_fontsize = max(5.5, min(8.5, 150 / max(n, 1)))
    bar_height = min(0.62, 13 / max(n, 1))

    # zebra striping behind the bars for readability
    for y in y_pos:
        if y % 2 == 0:
            ax.axhspan(y - 0.5, y + 0.5, color="#F2F2F2", zorder=0)

    bars = ax.barh(y_pos, values, color=STYLE["bar_color"], height=bar_height,
                    zorder=3, edgecolor="white", linewidth=0.4)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=label_fontsize, family=STYLE["font_family"])
    ax.set_xlabel("Number of projects", fontsize=6, family=STYLE["font_family"],
                   color="#444444")
    ax.tick_params(axis="x", labelsize=8, colors="#444444")
    ax.tick_params(axis="y", length=0)

    ax.grid(axis="x", color="#E3E3E3", linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for spine in ["top", "right", "left"]:
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color("#AAAAAA")

    max_val = max(values) if values else 1
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + max_val * 0.018, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", fontsize=max(6, label_fontsize),
                fontweight="bold", family=STYLE["font_family"], color="#333333")
    ax.set_xlim(0, max_val * 1.15)
    ax.set_ylim(-0.7, n - 0.3)

    add_header_footer(fig, f"{repo_name} -- Histogram of Primary Classes", page_num, total_pages)

    pdf.savefig(fig)
    plt.close(fig)


def add_table_page(pdf, repo_name, counts, page_num, total_pages, top_n=20):
    top = counts[:top_n]
    fig = plt.figure(figsize=PAGE_SIZE)
    ax = fig.add_axes([0.08, CONTENT_BOTTOM, 0.84, CONTENT_TOP - CONTENT_BOTTOM])
    ax.axis("off")

    table_data = [["Rank", "Class", "Count"]]
    for i, (code, count) in enumerate(top, start=1):
        table_data.append([str(i), class_label(code, wrap_width=70), str(count)])

    table = ax.table(cellText=table_data, cellLoc="left",
                      colWidths=[0.08, 0.78, 0.14], loc="upper center")
    table.auto_set_font_size(False)
    table.set_fontsize(STYLE["label_fontsize"])
    table.scale(1, 1.7)

    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor(STYLE["table_header_bg"])
            cell.set_text_props(color=STYLE["table_header_text"], fontweight="bold",
                                 family=STYLE["font_family"])
        else:
            cell.set_facecolor(STYLE["table_row_alt_bg"] if row % 2 == 0 else "white")
            cell.set_text_props(family=STYLE["font_family"])

    add_header_footer(fig, f"{repo_name} -- Top {len(top)} Classes, Rank-Ordered",
                       page_num, total_pages)

    pdf.savefig(fig)
    plt.close(fig)


def add_comments_page(pdf, repo_id, repo_name, page_num, total_pages):
    fig = plt.figure(figsize=PAGE_SIZE)
    ax = fig.add_axes([0.10, CONTENT_BOTTOM, 0.80, CONTENT_TOP - CONTENT_BOTTOM])
    ax.axis("off")

    raw_text = FINAL_COMMENTS.get(repo_id, "[No commentary written for this repository yet.]")
    wrapped_paragraphs = [
        "\n".join(textwrap.wrap(p, width=95)) if p else ""
        for p in raw_text.split("\n\n")
    ]
    text = "\n\n".join(wrapped_paragraphs)

    ax.text(0.0, 0.98, text, fontsize=STYLE["comment_fontsize"], va="top", ha="left",
            transform=ax.transAxes, family=STYLE["font_family"])

    add_header_footer(fig, f"{repo_name} -- Comments on Findings", page_num, total_pages)

    pdf.savefig(fig)
    plt.close(fig)


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT DISTINCT repository_id FROM projects ORDER BY repository_id")
    repo_ids = [r[0] for r in cur.fetchall()]

    all_counts = {repo_id: get_class_counts(cur, repo_id) for repo_id in repo_ids}
    repo_ids = [r for r in repo_ids if all_counts[r]]  # drop repos with no data

    # Every section is exactly one page, so total page count is deterministic:
    # 1 cover + 1 TOC + 3 pages per repository (histogram, table, comments)
    total_pages = 2 + 3 * len(repo_ids)

    # build TOC entries and page numbers up front
    toc_entries = []
    page_num = 3
    page_map = {}  # repo_id -> (hist_page, table_page, comments_page)
    for repo_id in repo_ids:
        repo_name = REPO_NAMES.get(repo_id, f"Repository {repo_id}")
        toc_entries.append((repo_name, page_num, 0))
        toc_entries.append((f"Histogram of Primary Classes", page_num, 1))
        toc_entries.append((f"Rank-Ordered Class Table", page_num + 1, 1))
        toc_entries.append((f"Comments on Findings", page_num + 2, 1))
        page_map[repo_id] = (page_num, page_num + 1, page_num + 2)
        page_num += 3

    with PdfPages(output_path) as pdf:
        add_cover_page(pdf)
        add_toc_page(pdf, toc_entries, total_pages)

        for repo_id in repo_ids:
            repo_name = REPO_NAMES.get(repo_id, f"Repository {repo_id}")
            counts = all_counts[repo_id]
            hist_pg, table_pg, comments_pg = page_map[repo_id]

            add_histogram_page(pdf, repo_name, counts, hist_pg, total_pages)
            add_table_page(pdf, repo_name, counts, table_pg, total_pages)
            add_comments_page(pdf, repo_id, repo_name, comments_pg, total_pages)

    con.close()
    print(f"Wrote report to {output_path}")
    print(f"Total pages: {total_pages}")
    print(f"Repositories included: {repo_ids}")


if __name__ == "__main__":
    main()
