from __future__ import annotations

from urllib.parse import urlencode


def build_pagination(
    *,
    base_path: str,
    page: int,
    page_size: int,
    total_items: int,
    params: dict[str, str],
) -> dict[str, object]:
    total_pages = max(1, (total_items + page_size - 1) // page_size) if total_items else 1
    current_page = min(max(page, 1), total_pages)
    start_index = 0 if total_items == 0 else ((current_page - 1) * page_size) + 1
    end_index = min(current_page * page_size, total_items)

    def page_url(page_number: int) -> str:
        query = {key: value for key, value in params.items() if value not in ("", None)}
        query["page"] = str(page_number)
        return f"{base_path}?{urlencode(query)}"

    window_start = max(1, current_page - 2)
    window_end = min(total_pages, current_page + 2)
    pages = [
        {"number": page_number, "url": page_url(page_number), "current": page_number == current_page}
        for page_number in range(window_start, window_end + 1)
    ]

    return {
        "page": current_page,
        "page_size": page_size,
        "total_items": total_items,
        "total_pages": total_pages,
        "start_index": start_index,
        "end_index": end_index,
        "pages": pages,
        "prev_url": page_url(current_page - 1) if current_page > 1 else None,
        "next_url": page_url(current_page + 1) if current_page < total_pages else None,
    }
