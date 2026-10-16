# -*- coding: utf-8 -*-
"""Свойства файла (Автор, Кем изменён, даты, программа) для docx/xlsm.
python-docx оставляет в отчёте автора «python-docx» и дату 2013 года, Excel - имя учётной записи
Windows, поэтому после сохранения свойства переписываются напрямую в docProps/*.xml.
"""
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

AUTHOR = "Семенов А. М."

CORE = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        '<dc:creator>{author}</dc:creator><cp:lastModifiedBy>{author}</cp:lastModifiedBy>'
        '<dcterms:created xsi:type="dcterms:W3CDTF">{created}</dcterms:created>'
        '<dcterms:modified xsi:type="dcterms:W3CDTF">{modified}</dcterms:modified>'
        '</cp:coreProperties>')


def fix_meta(path, author=AUTHOR):
    path = Path(path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with zipfile.ZipFile(path) as z:
        items = [(i, z.read(i.filename)) for i in z.infolist()]
    core = next(d for i, d in items if i.filename == "docProps/core.xml").decode("utf-8")
    m = re.search(r"<dcterms:created[^>]*>([^<]+)</dcterms:created>", core)
    created = m.group(1) if m and m.group(1)[:4] >= "2026" else now  # 2013 год из шаблона python-docx
    new_core = CORE.format(author=author, created=created, modified=now).encode("utf-8")

    tmp = path.with_suffix(path.suffix + ".tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as out:
        for info, data in items:
            if info.filename == "docProps/core.xml":
                data = new_core
            elif info.filename == "docProps/app.xml" and path.suffix == ".docx":
                app = data.decode("utf-8")
                app = re.sub(r"<Application>[^<]*</Application>", "<Application>Microsoft Office Word</Application>", app)
                app = re.sub(r"<AppVersion>[^<]*</AppVersion>", "<AppVersion>16.0000</AppVersion>", app)
                data = app.encode("utf-8")
            out.writestr(info, data)
    tmp.replace(path)


if __name__ == "__main__":
    import sys
    for p in sys.argv[1:]:
        fix_meta(p)
        print("ok:", p)
