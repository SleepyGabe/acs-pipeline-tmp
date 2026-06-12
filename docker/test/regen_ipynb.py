#!/usr/bin/env python3
"""
Regenerate oracle_to_postgres_migration.ipynb from the canonical .py.

Mirrors the documented convention (HANDOVER.md): split the .py on
`\\n# COMMAND ----------\\n`; `# MAGIC %md` chunks become markdown cells,
`# MAGIC %pip` / `%restart_python` chunks become code cells with the magics,
everything else becomes a code cell. The .py is the source of truth.

Usage:
  python docker/test/regen_ipynb.py <input.py> <output.ipynb>
Validate (should be a no-op diff against the committed pair):
  python docker/test/regen_ipynb.py oracle_to_postgres_migration.py /tmp/out.ipynb
"""
import json
import sys


def _strip_magic(line: str) -> str:
    # "# MAGIC xyz" -> "xyz"; "# MAGIC" -> ""
    if line.startswith("# MAGIC %md"):
        return None  # marker line, dropped
    if line.startswith("# MAGIC "):
        return line[len("# MAGIC "):]
    if line == "# MAGIC":
        return ""
    return line


def _to_source_list(text: str):
    """nbformat source: list of lines, each keeping its trailing newline except the last."""
    if text == "":
        return []
    return text.splitlines(keepends=True)


def build_notebook(py_text: str) -> dict:
    prefix = "# Databricks notebook source\n"
    if py_text.startswith(prefix):
        py_text = py_text[len(prefix):]

    cells_src = py_text.split("\n# COMMAND ----------\n")
    cells = []
    for raw in cells_src:
        block = raw.strip("\n")
        lines = block.split("\n")
        is_md = any(l.startswith("# MAGIC %md") for l in lines)
        is_magic = any(l.startswith("# MAGIC %") and not l.startswith("# MAGIC %md")
                       for l in lines)

        if is_md:
            out = []
            for l in lines:
                s = _strip_magic(l)
                if s is not None:
                    out.append(s)
            text = "\n".join(out).strip("\n")
            cells.append({
                "cell_type": "markdown",
                "metadata": {},
                "source": _to_source_list(text),
            })
        elif is_magic:
            out = [_strip_magic(l) for l in lines]
            out = [o for o in out if o is not None]
            text = "\n".join(out).strip("\n")
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": _to_source_list(text),
            })
        else:
            text = block.strip("\n")
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": _to_source_list(text),
            })

    return {
        "cells": cells,
        "metadata": {
            "application/vnd.databricks.v1+notebook": {
                "notebookName": "oracle_to_postgres_migration",
                "language": "python",
            },
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.13.13",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }


def main():
    inp, outp = sys.argv[1], sys.argv[2]
    with open(inp, encoding="utf-8") as f:
        nb = build_notebook(f.read())
    with open(outp, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
        f.write("\n")


if __name__ == "__main__":
    main()
