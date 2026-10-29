"""Helpers to save small result tables as CSV and Markdown (for REPORT.md)."""
import pandas as pd

from common.paths import output_dir


def to_markdown(pdf, float_digits=2):
    """Render a pandas DataFrame as a GitHub-flavoured Markdown table."""
    # Nullable dtypes keep integer columns with nulls as integers (2017, not 2017.0).
    pdf = pdf.convert_dtypes()
    headers = [str(c) for c in pdf.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in pdf.itertuples(index=False):
        cells = []
        for value in row:
            if value is pd.NA or (isinstance(value, float) and pd.isna(value)):
                cells.append("")
            elif isinstance(value, float):
                cells.append(f"{value:.{float_digits}f}")
            else:
                cells.append(str(value).replace("|", "\\|").replace("\n", " "))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def save_table(pdf, section, name, float_digits=2):
    """Write <output>/<section>/<name>.csv and .md, return the Markdown text."""
    folder = output_dir(section)
    pdf.convert_dtypes().to_csv(folder / f"{name}.csv", index=False)
    markdown = to_markdown(pdf, float_digits)
    (folder / f"{name}.md").write_text(markdown, encoding="utf-8")
    print(f"Saved {folder / name}.csv and .md ({len(pdf)} rows)")
    return markdown
