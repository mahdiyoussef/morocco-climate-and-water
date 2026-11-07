"""Refreshing the screenshot gallery must preserve handwritten README content."""
from scripts.capture_dashboard import START, END, gallery_markdown, update_readme


def test_gallery_refresh_is_idempotent_and_keeps_manual_content(tmp_path):
    path = tmp_path / 'README.md'
    path.write_text('# Project\n\nHandwritten introduction.\n\n## Actual data\nKeep these data notes.\n')
    first = gallery_markdown()
    update_readme(path, first)
    update_readme(path, first)
    text = path.read_text()
    assert text.count(START) == text.count(END) == 1
    assert text.count('docs/screenshots/overview.png') == 1
    assert 'Handwritten introduction.' in text and 'Keep these data notes.' in text
    second = first.replace('Representative desktop views', 'Refreshed desktop views')
    update_readme(path, second)
    assert 'Refreshed desktop views' in path.read_text()
    assert 'Representative desktop views' not in path.read_text()
