from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BookInfo:
    title: str
    author: str
    hook_intro: str


@dataclass
class Chapter:
    chapter_id: int
    chapter_title: str
    chapter_intro: str
    breakdown: dict[str, str]


@dataclass
class Book:
    book_info: BookInfo
    chapters: list[Chapter]
    outro: str
    json_path: str = ""

    @property
    def slug(self) -> str:
        cleaned = "".join(
            c if c.isalnum() or c in " _-" else "_"
            for c in self.book_info.title
        )
        return cleaned.strip("_").replace(" ", "_").lower() or "unknown_book"

    @property
    def chapter_count(self) -> int:
        return len(self.chapters)

    def is_first_chapter(self, index: int) -> bool:
        return index == 0

    def is_last_chapter(self, index: int) -> bool:
        return index == self.chapter_count - 1
