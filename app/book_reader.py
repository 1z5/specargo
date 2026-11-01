from __future__ import annotations

import json
from pathlib import Path

from models import Book, BookInfo, Chapter


def read_book(json_path: Path) -> Book:
    data = json.loads(json_path.read_text(encoding="utf-8"))

    book_info_raw = data.get("book_info", {})
    book_info = BookInfo(
        title=book_info_raw.get("title", ""),
        author=book_info_raw.get("author", ""),
        hook_intro=book_info_raw.get("hook_intro", ""),
    )

    chapters: list[Chapter] = []
    for ch_raw in data.get("chapters", []):
        chapters.append(Chapter(
            chapter_id=ch_raw.get("chapter_id", 0),
            chapter_title=ch_raw.get("chapter_title", ""),
            chapter_intro=ch_raw.get("chapter_intro", ""),
            breakdown=ch_raw.get("breakdown", {}),
        ))

    outro = data.get("outro", "")

    return Book(
        book_info=book_info,
        chapters=chapters,
        outro=outro,
        json_path=str(json_path),
    )


def find_book_json_files(books_dir: Path) -> list[Path]:
    if not books_dir.exists():
        return []

    result: list[Path] = []

    for item in sorted(books_dir.iterdir()):
        if item.is_file() and item.suffix == ".json":
            result.append(item)
        elif item.is_dir():
            result.extend(find_book_json_files(item))

    return result
