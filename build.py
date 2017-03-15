#!/usr/bin/env python
"""
build.py

Build HTML output
"""
from __future__ import absolute_import, division, print_function

import re
import os
import argparse
import subprocess
import shutil
import lxml.html


def main():
    p = argparse.ArgumentParser(usage=__doc__.rstrip())
    p.add_argument('--html', action='store_true', help="Build HTML output")
    p.add_argument('--no-clean', action='store_true', help="Skip removing old output")
    args = p.parse_args()

    os.chdir(os.path.abspath(os.path.dirname(__file__)))

    dst_path = os.path.join('docs', 'items')
    dst_path_ipynb = os.path.join('docs', 'static', 'items')

    if os.path.isdir(dst_path) and not args.no_clean:
        shutil.rmtree(dst_path)
    if not os.path.isdir(dst_path):
        os.makedirs(dst_path)
    if os.path.isdir(dst_path_ipynb):
        shutil.rmtree(dst_path_ipynb)

    shutil.copytree(os.path.join('notebooks', 'attachments'),
                    os.path.join(dst_path, 'attachments'))

    shutil.copytree('notebooks', dst_path_ipynb,
                    ignore=lambda src, names: [x for x in names if not x.endswith('.ipynb')])

    titles, tags = generate_files(dst_path=dst_path)

    write_index(dst_path, titles, tags)

    if args.html:
        subprocess.check_call(['sphinx-build', '-b', 'html', '.', '_build/html'],
                              cwd='docs')


def write_index(dst_path, titles, tags):
    """
    Write index files under `dst_path`

    """
    titles = dict(titles)

    index_rst = os.path.join(dst_path, 'index.txt')

    # Write doctree
    toctree_items = []
    index_text = []

    # Fill in missing tags
    for fn in titles.keys():
        if fn not in tags or not tags[fn]:
            tags[fn] = ['Other examples']

    # Count tags
    tag_counts = {}
    for fn, tagset in tags.items():
        for tag in tagset:
            if tag not in tag_counts:
                tag_counts[tag] = 1
            else:
                tag_counts[tag] += 1
    tag_counts['Outdated'] = 1e99

    # Generate tree
    def get_section_name(tag_id):
        return ("idx_" +
                re.sub('_+', '_', re.sub('[^a-z0-9_]', "_", "_" + tag_id.lower())).strip('_'))

    tag_sets = {}
    for fn, tagset in tags.items():
        tagset = list(set(tagset))
        tagset.sort(key=lambda tag: -tag_counts[tag])
        if 'Outdated' in tagset:
            tagset = ['Outdated']
        tag_id = " / ".join(tagset[:2])
        tag_sets.setdefault(tag_id, set()).add(fn)

        if len(tagset[:2]) > 1:
            # Add sub-tag to the tree
            sec_name = get_section_name(tag_id)
            titles[sec_name] = tagset[1]
            tag_sets.setdefault(tagset[0], set()).add(sec_name)

    tag_sets = list(tag_sets.items())
    def tag_set_sort(item):
        return (1 if 'Outdated' in item[0] else 0,
                item)
    tag_sets.sort(key=tag_set_sort)

    # Produce output
    for tag_id, fns in tag_sets:
        fns = list(fns)
        fns.sort(key=lambda fn: titles[fn])

        section_base_fn = get_section_name(tag_id)
        section_fn = os.path.join(dst_path, section_base_fn + '.rst')

        if ' / ' not in tag_id:
            toctree_items.append(section_base_fn)

        non_idx_items = [fn for fn in fns if not fn.startswith('idx_')]

        if non_idx_items:
            index_text.append("\n{0}\n{1}\n\n".format(tag_id, "-"*len(tag_id)))
            for fn in non_idx_items:
                index_text.append(":doc:`{0} <items/{1}>`\n".format(titles[fn], fn))

        with open(section_fn, 'w') as f:
            sec_title = titles.get(section_base_fn, tag_id)
            f.write("{0}\n{1}\n\n".format(sec_title, "="*len(sec_title)))

            sub_idx = [fn for fn in fns if fn.startswith('idx')]

            if sub_idx:
                f.write(".. toctree::\n"
                        "   :maxdepth: 1\n\n")
                for fn in sub_idx:
                    f.write("   {0}\n".format(fn))
                f.write("\n\n")

            f.write(".. toctree::\n"
                    "   :maxdepth: 1\n\n")
            for fn in fns:
                if fn in sub_idx:
                    continue
                f.write("   {0}\n".format(fn))

    # Write index
    with open(index_rst, 'w') as f:
        f.write(".. toctree::\n"
                "   :maxdepth: 1\n"
                "   :hidden:\n\n")
        for fn in toctree_items:
            f.write("   items/%s\n" % (fn,))
        f.write("\n\n")
        f.write('.. raw:: html\n\n   <div id="cookbook-index">\n\n')
        f.write("".join(index_text))
        f.write('\n\n.. raw:: html\n\n   </div>\n')
        f.close()


def generate_files(dst_path):
    """
    Read all .ipynb files and produce .rst under `dst_path`

    Returns
    -------
    titles : dict
        Dictionary {file_basename: notebook_title, ...}
    tags : dict
        Dictionary {file_basename: set([tag1, tag2, ...]), ...}
    """
    titles = {}
    tags = {}

    for fn in sorted(os.listdir('notebooks')):
        if not fn.endswith('.ipynb'):
            continue
        fn = os.path.join('notebooks', fn)
        basename = os.path.splitext(os.path.basename(fn))[0]

        editors = list()

        # Get names from Git
        p = subprocess.Popen(['git', 'log', '--format=%an', 'ef45029096..', fn],
                                 stdout=subprocess.PIPE)
        names, _ = p.communicate()
        for name in names.splitlines():
            name = name.strip()
            if name and name not in editors:
                editors.append(name)

        # Continue
        title, tagset = convert_file(dst_path, fn, editors)
        titles[basename] = title
        if tagset:
            tags[basename] = tagset

    return titles, tags


def convert_file(dst_path, fn, editors):
    """
    Convert .ipynb to .rst, placing output under `dst_path`

    Returns
    -------
    title : str
        Title of the notebook
    tags : set of str
        Tags given in the notebook file

    """
    print(fn)
    subprocess.check_call(['jupyter', 'nbconvert', '--to', 'html',
                           '--output-dir', os.path.abspath(dst_path),
                           os.path.abspath(fn)],
                          cwd=dst_path, stderr=subprocess.STDOUT)

    basename = os.path.splitext(os.path.basename(fn))[0]
    rst_fn = os.path.join(dst_path, basename + '.rst')
    html_fn = os.path.join(dst_path, basename + '.html')

    title = None
    tags = set()
    editors = list(editors)
    legacy_editors = True

    lines = []

    # Parse and munge HTML
    tree = lxml.html.parse(html_fn)
    os.unlink(html_fn)

    root = tree.getroot()
    head = root.find('head')
    container, = root.xpath("//div[@id='notebook-container']")

    headers = container.xpath('//h1')
    if headers:
        title = headers[0].text
        #if isinstance(title, unicode):
        #    title = title.encode('utf-8')
        h1_parent = headers[0].getparent()
        h1_parent.remove(headers[0])

    lines.extend([".. raw:: html", ""])

    for element in head.getchildren():
        if element.tag in ('script',):
            text = str(lxml.html.tostring(element))
            lines.extend("   " + x for x in text.splitlines())

    text = str(lxml.html.tostring(container))

    m = re.search(r'<p>TAGS:\s*(.*)\s*</p>', text)
    if m:
        tag_line = m.group(1).strip().replace(';', ',')
        if isinstance(tag_line, unicode):
            tag_line = tag_line.encode('utf-8')
        tags.update([x.strip() for x in tag_line.split(",")])
        text = text[:m.start()] + text[m.end():]

    m = re.search(r'<p>AUTHORS:\s*(.*)\s*</p>', text)
    if m:
        # Author lines override editors
        if legacy_editors:
            editors = []
            legacy_editors = False
        author_line = m.group(1).strip().replace(';', ',')
        if isinstance(author_line, unicode):
            author_line = author_line.encode('utf-8')
        for author in author_line.split(","):
            author = author.strip()
            if author and author not in editors:
                editors.append(author)

        text = text[:m.start()] + text[m.end():]

    text = text.replace(u'attachments/{0}/'.format(basename),
                        u'../_downloads/')

    lines.extend(u"   " + x for x in text.splitlines())
    lines.append(u"")

    # Produce output
    text = u"\n".join(lines).encode('utf-8')

    if not title:
        title = basename

    authors = ", ".join(editors)
    text = "{0}\n{1}\n\n{2}".format(title, "="*len(title), text)

    with open(rst_fn, 'w') as f:
        f.write(text)
        if authors:
            f.write("\n\n.. sectionauthor:: {0}".format(authors))
    del text

    attach_dir = os.path.join('notebooks', 'attachments', basename)
    if os.path.isdir(attach_dir) and len(os.listdir(attach_dir)) > 0:
        with open(rst_fn, 'a') as f:
            f.write("""

.. rubric:: Attachments

""")
            for fn in sorted(os.listdir(attach_dir)):
                if os.path.isfile(os.path.join(attach_dir, fn)):
                    f.write('- :download:`%s <attachments/%s/%s>`\n' % (
                        fn, basename, fn))

    return title, tags


if __name__ == "__main__":
    main()
