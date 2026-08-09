from crawler.directory import parse_directory_html

_HTML = """\
<html><head><title>download.bls.gov - /pub/time.series/pr/</title></head>
<body><H1>download.bls.gov - /pub/time.series/pr/</H1><hr>
<pre><A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>
  8/6/2026  8:30 AM          102 <A HREF="/pub/time.series/pr/pr.class">pr.class</A><br>
  8/6/2026  8:30 AM      1613575 <A HREF="/pub/time.series/pr/pr.data.0.Current">pr.data.0.Current</A><br>
  9/13/2022  4:52 PM          562 <A HREF="/pub/time.series/pr/pr.contacts">pr.contacts</A><br>
</pre></body></html>"""


def test_parses_file_count():
    entries = parse_directory_html(_HTML)
    assert len(entries) == 3


def test_parent_link_excluded():
    entries = parse_directory_html(_HTML)
    assert not any(e.filename == "[To Parent Directory]" for e in entries)
    assert not any("/pub/time.series/" == e.url for e in entries)


def test_entry_fields():
    entries = parse_directory_html(_HTML)
    e = next(x for x in entries if x.filename == "pr.class")
    assert e.url == "https://download.bls.gov/pub/time.series/pr/pr.class"
    assert "8/6/2026" in e.last_modified
    assert e.size_bytes == 102


def test_large_file_size():
    entries = parse_directory_html(_HTML)
    e = next(x for x in entries if x.filename == "pr.data.0.Current")
    assert e.size_bytes == 1613575


def test_empty_html():
    assert parse_directory_html("") == []


def test_filenames_stripped():
    entries = parse_directory_html(_HTML)
    assert all(e.filename == e.filename.strip() for e in entries)
