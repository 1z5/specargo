from __future__ import annotations

from models import Book, Chapter


def _join_breakdown(breakdown: dict[str, str]) -> str:
    items = sorted(
        breakdown.items(),
        key=lambda kv: (
            int(kv[0].split("_", 1)[0])
            if kv[0].split("_", 1)[0].isdigit()
            else 999
        ),
    )
    parts = [v.strip() for _, v in items if v.strip()]
    return "\n\n".join(parts)


OPENING_TEMPLATE = "Let's start the book, {title} by {author}.\n\n{hook_intro}"
CHAPTER_TEMPLATE = "Point {chapter_id}, {chapter_title}. {chapter_intro}\n\n"
CLOSING_TEMPLATE = (
    "You have reached the conclusion of this book summary. {outro}\n\n"
    "Thank you for listening to this book summary. See you in the next book."
)


def compose_opening(book: Book) -> str:
    return OPENING_TEMPLATE.format(
        title=book.book_info.title,
        author=book.book_info.author,
        hook_intro=book.book_info.hook_intro,
    ).strip()


def compose_chapter(chapter: Chapter) -> str:
    parts = [
        CHAPTER_TEMPLATE.format(
            chapter_id=chapter.chapter_id,
            chapter_title=chapter.chapter_title,
            chapter_intro=chapter.chapter_intro,
        ),
    ]
    breakdown_text = _join_breakdown(chapter.breakdown)
    if breakdown_text:
        parts.append(breakdown_text)
    return "".join(parts).strip()


def compose_closing(book: Book) -> str:
    return CLOSING_TEMPLATE.format(outro=book.outro).strip()
