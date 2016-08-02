"""
Markdown document tree writer
"""

__docformat__ = 'reStructuredText'


import sys
import os
import os.path
import time
import re
import urllib
try: # check for the Python Imaging Library
    import PIL.Image
except ImportError:
    try:  # sometimes PIL modules are put in PYTHONPATH's root
        import Image
        class PIL(object): pass  # dummy wrapper
        PIL.Image = Image
    except ImportError:
        PIL = None
import docutils
from docutils import frontend, nodes, utils, writers, languages, io
from docutils.utils.error_reporting import SafeString
from docutils.transforms import writer_aux
from docutils.utils.math import unichar2tex, pick_math_environment, math2html
from docutils.utils.math.latex2mathml import parse_latex_math

class Writer(writers.Writer):

    supported = ('markdown')
    """Formats this writer supports."""

    default_template = 'catalog/template.txt'

    default_template_path = utils.relative_path(
        os.path.join(os.getcwd(), 'dummy'),
        os.path.join(os.path.dirname(__file__), default_template))

    settings_spec = (
        'MD-Specific Options',
        None,
        (('Specify the template file (UTF-8 encoded).  Default is "%s".'
          % default_template_path,
          ['--template'],
          {'default': default_template_path, 'metavar': '<file>'}),
         ('Specify the initial header level.  Default is 1 for "<h1>".  '
          'Does not affect document title & subtitle (see --no-doc-title).',
          ['--initial-header-level'],
          {'choices': '1 2 3 4 5 6'.split(), 'default': '1',
           'metavar': '<level>'}),
         ('Specify the maximum width (in characters) for one-column field '
          'names.  Longer field names will span an entire row of the table '
          'used to render the field list.  Default is 14 characters.  '
          'Use 0 for "no limit".',
          ['--field-name-limit'],
          {'default': 14, 'metavar': '<level>',
           'validator': frontend.validate_nonnegative_int}),
         ('Specify the maximum width (in characters) for options in option '
          'lists.  Longer options will span an entire row of the table used '
          'to render the option list.  Default is 14 characters.  '
          'Use 0 for "no limit".',
          ['--option-limit'],
          {'default': 14, 'metavar': '<level>',
           'validator': frontend.validate_nonnegative_int}),
         ('Format for footnote references: one of "superscript" or '
          '"brackets".  Default is "brackets".',
          ['--footnote-references'],
          {'choices': ['superscript', 'brackets'], 'default': 'brackets',
           'metavar': '<format>',
           'overrides': 'trim_footnote_reference_space'}),
         ('Format for block quote attributions: one of "dash" (em-dash '
          'prefix), "parentheses"/"parens", or "none".  Default is "dash".',
          ['--attribution'],
          {'choices': ['dash', 'parentheses', 'parens', 'none'],
           'default': 'dash', 'metavar': '<format>'}),
         ('Remove extra vertical whitespace between items of "simple" bullet '
          'lists and enumerated lists.  Default: enabled.',
          ['--compact-lists'],
          {'default': 1, 'action': 'store_true',
           'validator': frontend.validate_boolean}),
         ('Disable compact simple bullet and enumerated lists.',
          ['--no-compact-lists'],
          {'dest': 'compact_lists', 'action': 'store_false'}),
         ('Remove extra vertical whitespace between items of simple field '
          'lists.  Default: enabled.',
          ['--compact-field-lists'],
          {'default': 1, 'action': 'store_true',
           'validator': frontend.validate_boolean}),
         ('Disable compact simple field lists.',
          ['--no-compact-field-lists'],
          {'dest': 'compact_field_lists', 'action': 'store_false'}),
         ('Added to standard table classes. '
          'Defined styles: "borderless". Default: ""',
          ['--table-style'],
          {'default': ''}),
         ('Math output format, one of "MathML", "HTML", "MathJax" '
          'or "LaTeX". Default: "HTML math.css"',
          ['--math-output'],
          {'default': 'HTML math.css'}),
         ('Obfuscate email addresses to confuse harvesters while still '
          'keeping email links usable with standards-compliant browsers.',
          ['--cloak-email-addresses'],
          {'action': 'store_true', 'validator': frontend.validate_boolean}),))

    settings_defaults = {'output_encoding_error_handler': 'xmlcharrefreplace'}

    config_section = 'markdown writer'
    config_section_dependencies = ('writers',)

    visitor_attributes = (
        'body_pre_docinfo', 'docinfo', 'body',
        'title', 'subtitle', 'header', 'footer', 'fragment',)

    def get_transforms(self):
        return writers.Writer.get_transforms(self) + [writer_aux.Admonitions]

    def __init__(self):
        writers.Writer.__init__(self)
        self.translator_class = MarkdownTranslator

    def translate(self):
        self.visitor = visitor = self.translator_class(self.document)
        self.document.walkabout(visitor)
        for attr in self.visitor_attributes:
            setattr(self, attr, getattr(visitor, attr))
        self.output = self.apply_template()

    def apply_template(self):
        template_file = open(self.document.settings.template, 'rb')
        template = unicode(template_file.read(), 'utf-8')
        template_file.close()
        subs = self.interpolation_dict()
        return template % subs

    def interpolation_dict(self):
        subs = {}
        settings = self.document.settings
        for attr in self.visitor_attributes:
            subs[attr] = ''.join(getattr(self, attr)).rstrip('\n')
        subs['encoding'] = settings.output_encoding
        subs['version'] = docutils.__version__
        return subs

    def assemble_parts(self):
        writers.Writer.assemble_parts(self)
        for part in self.visitor_attributes:
            self.parts[part] = ''.join(getattr(self, part))


class MarkdownTranslator(nodes.NodeVisitor):

    words_and_spaces = re.compile(r'\S+| +|\n')
    sollbruchstelle = re.compile(r'.+\W\W.+|[-?].+', re.U) # wrap point inside word
    lang_attribute = 'lang' # name changes to 'xml:lang' in XHTML 1.1

    def __init__(self, document, *args, **kwds):
        nodes.NodeVisitor.__init__(self, document)
        self.settings = settings = document.settings
        lcode = settings.language_code
        self.language = languages.get_language(lcode, document.reporter)
        self.body_pre_docinfo = []
        # author, date, etc.
        self.docinfo = []
        self.body = []
        self.fragment = []
        self.section_level = 0
        self.initial_header_level = int(settings.initial_header_level)

        self.math_output = settings.math_output.split()
        self.math_output_options = self.math_output[1:]
        self.math_output = self.math_output[0].lower()

        # A heterogenous stack used in conjunction with the tree traversal.
        # Make sure that the pops correspond to the pushes:
        self.context = []
        self.topic_classes = []
        self.colspecs = []
        self.compact_p = True
        self.compact_simple = False
        self.compact_field_list = False
        self.in_docinfo = False
        self.in_sidebar = False
        self.title = []
        self.subtitle = []
        self.header = []
        self.footer = []
        self.in_document_title = 0   # len(self.body) or 0
        self.in_mailto = False
        self.author_in_authors = False
        self.math_header = []

        self.no_smarty = 0
        self.protect_literal_text = 0
        self.param_separator = ''
        self.optional_param_level = 0
        self._table_row_index = 0
        self.list_level = 0

    def astext(self):
        return ''.join(self.body_pre_docinfo + self.docinfo + self.body)

    def encode(self, text):
        """Encode special characters in `text` & return."""
        # @@@ A codec to do these and all other HTML entities would be nice.
        text = unicode(text)
        return text.translate({
            ord('&'): u'&amp;',
            ord('<'): u'&lt;',
            ord('"'): u'&quot;',
            ord('>'): u'&gt;',
            ord('@'): u'&#64;', # may thwart some address harvesters
            # TODO: convert non-breaking space only if needed?
            0xa0: u'&nbsp;'}) # non-breaking space

    def attval(self, text,
               whitespace=re.compile('[\n\r\t\v\f]')):
        """Cleanse, HTML encode, and return attribute value text."""
        encoded = self.encode(whitespace.sub(' ', text))
        return encoded

    def starttag(self, node, tagname, suffix='\n', empty=False, **attributes):
        """
        Construct and return a start tag given a node (id & class attributes
        are extracted), tag name, and optional attributes.
        """
        tagname = tagname.lower()
        prefix = []
        atts = {}
        ids = []
        for (name, value) in attributes.items():
            atts[name.lower()] = value
        classes = []
        languages = []
        # unify class arguments and move language specification
        for cls in node.get('classes', []) + atts.pop('class', '').split() :
            if cls.startswith('language-'):
                languages.append(cls[9:])
            elif cls.strip() and cls not in classes:
                classes.append(cls)
        if languages:
            # attribute name is 'lang' in XHTML 1.0 but 'xml:lang' in 1.1
            atts[self.lang_attribute] = languages[0]
        if classes:
            atts['class'] = ' '.join(classes)
        assert 'id' not in atts
        ids.extend(node.get('ids', []))
        if 'ids' in atts:
            ids.extend(atts['ids'])
            del atts['ids']
        if ids:
            atts['id'] = ids[0]
            for id in ids[1:]:
                # Add empty "span" elements for additional IDs.  Note
                # that we cannot use empty "a" elements because there
                # may be targets inside of references, but nested "a"
                # elements aren't allowed in XHTML (even if they do
                # not all have a "href" attribute).
                if empty:
                    # Empty tag.  Insert target right in front of element.
                    prefix.append('<span id="%s"></span>' % id)
                else:
                    # Non-empty tag.  Place the auxiliary <span> tag
                    # *inside* the element, as the first child.
                    suffix += '<span id="%s"></span>' % id
        attlist = atts.items()
        attlist.sort()
        parts = [tagname]
        for name, value in attlist:
            # value=None was used for boolean attributes without
            # value, but this isn't supported by XHTML.
            assert value is not None
            if isinstance(value, list):
                values = [unicode(v) for v in value]
                parts.append('%s="%s"' % (name.lower(),
                                          self.attval(' '.join(values))))
            else:
                parts.append('%s="%s"' % (name.lower(),
                                          self.attval(unicode(value))))
        if empty:
            infix = ' /'
        else:
            infix = ''
        return ''.join(prefix) + '<%s%s>' % (' '.join(parts), infix) + suffix

    def emptytag(self, node, tagname, suffix='\n', **attributes):
        """Construct and return an XML-compatible empty tag."""
        return self.starttag(node, tagname, suffix, empty=True, **attributes)

    def set_class_on_child(self, node, class_, index=0):
        """
        Set class `class_` on the visible child no. index of `node`.
        Do nothing if node has fewer children than `index`.
        """
        children = [n for n in node if not isinstance(n, nodes.Invisible)]
        try:
            child = children[index]
        except IndexError:
            return
        child['classes'].append(class_)

    def set_first_last(self, node):
        self.set_class_on_child(node, 'first', 0)
        self.set_class_on_child(node, 'last', -1)

    def visit_Text(self, node):
        text = node.astext()
        encoded = self.encode(text)
        if self.protect_literal_text:
            encoded = '\n'.join([
                '    {}'.format(line) for line in encoded.split('\n')
            ])
        else:
            encoded = self.bulk_text_processor(encoded)
        self.body.append(encoded)

    def depart_Text(self, node):
        pass

    def bulk_text_processor(self, text):
        return text

    def should_be_compact_paragraph(self, node):
        return True

    def visit_compact_paragraph(self, node):
        pass

    def depart_compact_paragraph(self, node):
        self.body.append('\n\n')

    def visit_abbreviation(self, node):
        # @@@ implementation incomplete ("title" attribute)
        self.body.append(self.starttag(node, 'abbr', ''))

    def depart_abbreviation(self, node):
        self.body.append('</abbr>')

    def visit_acronym(self, node):
        # @@@ implementation incomplete ("title" attribute)
        self.body.append(self.starttag(node, 'acronym', ''))

    def depart_acronym(self, node):
        self.body.append('</acronym>')

    def visit_address(self, node):
        self.visit_docinfo_item(node, 'address', meta=False)
        self.body.append(self.starttag(node, 'pre', CLASS='address'))

    def depart_address(self, node):
        self.body.append('\n</pre>\n')
        self.depart_docinfo_item()

    def visit_admonition(self, node):
        self.body.append(self.starttag(node, 'div'))
        self.set_first_last(node)

    def depart_admonition(self, node=None):
        self.body.append('</div>\n')

    attribution_formats = {'dash': ('&mdash;', ''),
                           'parentheses': ('(', ')'),
                           'parens': ('(', ')'),
                           'none': ('', '')}

    def visit_attribution(self, node):
        prefix, suffix = self.attribution_formats[self.settings.attribution]
        self.context.append(suffix)
        self.body.append(
            self.starttag(node, 'p', prefix, CLASS='attribution'))

    def depart_attribution(self, node):
        self.body.append(self.context.pop() + '</p>\n')

    def visit_author(self, node):
        if isinstance(node.parent, nodes.authors):
            if self.author_in_authors:
                self.body.append('\n<br />')
        else:
            self.visit_docinfo_item(node, 'author')

    def depart_author(self, node):
        if isinstance(node.parent, nodes.authors):
            self.author_in_authors = True
        else:
            self.depart_docinfo_item()

    def visit_authors(self, node):
        self.visit_docinfo_item(node, 'authors')
        self.author_in_authors = False  # initialize

    def depart_authors(self, node):
        self.depart_docinfo_item()

    def visit_block_quote(self, node):
        self.body.append(self.starttag(node, 'blockquote'))

    def depart_block_quote(self, node):
        self.body.append('</blockquote>\n')

    def check_simple_list(self, node):
        """Check for a simple list that can be rendered compactly."""
        visitor = SimpleListChecker(self.document)
        try:
            node.walk(visitor)
        except nodes.NodeFound:
            return None
        else:
            return 1

    def visit_bullet_list(self, node):
        self.list_level += 1

    def depart_bullet_list(self, node):
        self.list_level -= 1
        #self.body.append('\n')

    def visit_caption(self, node):
        self.body.append(self.starttag(node, 'p', '', CLASS='caption'))

    def depart_caption(self, node):
        self.body.append('</p>\n')

    def visit_citation(self, node):
        self.body.append(self.starttag(node, 'table',
                                       CLASS='docutils citation',
                                       frame="void", rules="none"))
        self.body.append('<colgroup><col class="label" /><col /></colgroup>\n'
                         '<tbody valign="top">\n'
                         '<tr>')
        self.footnote_backrefs(node)

    def depart_citation(self, node):
        self.body.append('</td></tr>\n'
                         '</tbody>\n</table>\n')

    def visit_citation_reference(self, node):
        href = '#'
        if 'refid' in node:
            href += node['refid']
        elif 'refname' in node:
            href += self.document.nameids[node['refname']]
        # else: # TODO system message (or already in the transform)?
        # 'Citation reference missing.'
        self.body.append(self.starttag(
            node, 'a', '[', CLASS='citation-reference', href=href))

    def depart_citation_reference(self, node):
        self.body.append(']</a>')

    def visit_classifier(self, node):
        self.body.append(' <span class="classifier-delimiter">:</span> ')
        self.body.append(self.starttag(node, 'span', '', CLASS='classifier'))

    def depart_classifier(self, node):
        self.body.append('</span>')

    def visit_colspec(self, node):
        self.colspecs.append(node)
        # "stubs" list is an attribute of the tgroup element:
        node.parent.stubs.append(node.attributes.get('stub'))

    def depart_colspec(self, node):
        pass

    def write_colspecs(self):
        width = 0
        for node in self.colspecs:
            width += node['colwidth']
        for node in self.colspecs:
            colwidth = int(node['colwidth'] * 100.0 / width + 0.5)
            self.body.append(self.emptytag(node, 'col',
                                           width='%i%%' % colwidth))
        self.colspecs = []

    def visit_comment(self, node,
                      sub=re.compile('-(?=-)').sub):
        """Escape double-dashes in comment text."""
        self.body.append('<!-- %s -->\n' % sub('- ', node.astext()))
        # Content already processed:
        raise nodes.SkipNode

    def visit_compound(self, node):
        pass

    def depart_compound(self, node):
        pass

    def visit_container(self, node):
        self.body.append(self.starttag(node, 'div', CLASS='container'))

    def depart_container(self, node):
        self.body.append('</div>\n')

    def visit_contact(self, node):
        self.visit_docinfo_item(node, 'contact', meta=False)

    def depart_contact(self, node):
        self.depart_docinfo_item()

    def visit_copyright(self, node):
        self.visit_docinfo_item(node, 'copyright')

    def depart_copyright(self, node):
        self.depart_docinfo_item()

    def visit_date(self, node):
        self.visit_docinfo_item(node, 'date')

    def depart_date(self, node):
        self.depart_docinfo_item()

    def visit_decoration(self, node):
        pass

    def depart_decoration(self, node):
        pass

    def visit_definition(self, node):
        self.body.append('</dt>\n')
        self.body.append(self.starttag(node, 'dd', ''))
        self.set_first_last(node)

    def depart_definition(self, node):
        self.body.append('</dd>\n')

    def visit_definition_list(self, node):
        self.body.append(self.starttag(node, 'dl', CLASS='docutils'))

    def depart_definition_list(self, node):
        self.body.append('</dl>\n')

    def visit_definition_list_item(self, node):
        pass

    def depart_definition_list_item(self, node):
        pass

    def visit_description(self, node):
        self.body.append(self.starttag(node, 'td', ''))
        self.set_first_last(node)

    def depart_description(self, node):
        self.body.append('</td>')

    def visit_docinfo(self, node):
        self.context.append(len(self.body))
        self.body.append(self.starttag(node, 'table',
                                       CLASS='docinfo',
                                       frame="void", rules="none"))
        self.body.append('<col class="docinfo-name" />\n'
                         '<col class="docinfo-content" />\n'
                         '<tbody valign="top">\n')
        self.in_docinfo = True

    def depart_docinfo(self, node):
        self.body.append('</tbody>\n</table>\n')
        self.in_docinfo = False
        start = self.context.pop()
        self.docinfo = self.body[start:]
        self.body = []

    def visit_docinfo_item(self, node, name, meta=True):
        if meta:
            meta_tag = '<meta name="%s" content="%s" />\n' \
                       % (name, self.attval(node.astext()))
            self.add_meta(meta_tag)
        self.body.append(self.starttag(node, 'tr', ''))
        self.body.append('<th class="docinfo-name">%s:</th>\n<td>'
                         % self.language.labels[name])
        if len(node):
            if isinstance(node[0], nodes.Element):
                node[0]['classes'].append('first')
            if isinstance(node[-1], nodes.Element):
                node[-1]['classes'].append('last')

    def depart_docinfo_item(self):
        self.body.append('</td></tr>\n')

    def visit_doctest_block(self, node):
        self.body.append(self.starttag(node, 'pre', CLASS='doctest-block'))

    def depart_doctest_block(self, node):
        self.body.append('\n</pre>\n')

    def visit_document(self, node):
        pass

    def depart_document(self, node):
        # skip content-type meta tag with interpolated charset value:
        #self.html_head.extend(self.head[1:])
        #self.body_prefix.append(self.starttag(node, 'div', CLASS='document'))
        #self.body_suffix.insert(0, '</div>\n')
        self.fragment.extend(self.body) # self.fragment is the "naked" body
        #self.html_body.extend(self.body_prefix[1:] + self.body_pre_docinfo
        #                      + self.docinfo + self.body
        #                      + self.body_suffix[:-1])
        assert not self.context, 'len(context) = %s' % len(self.context)

    def visit_emphasis(self, node):
        self.body.append(self.starttag(node, 'em', ''))

    def depart_emphasis(self, node):
        self.body.append('</em>')

    def visit_entry(self, node):
        #atts = {'class': []}
        #if isinstance(node.parent.parent, nodes.thead):
        #    atts['class'].append('head')
        #if node.parent.parent.parent.stubs[node.parent.column]:
        #    # "stubs" list is an attribute of the tgroup element
        #    atts['class'].append('stub')
        #if atts['class']:
        #    tagname = 'th'
        #    atts['class'] = ' '.join(atts['class'])
        #else:
        #    tagname = 'td'
        #    del atts['class']
        #node.parent.column += 1
        #if 'morerows' in node:
        #    atts['rowspan'] = node['morerows'] + 1
        #if 'morecols' in node:
        #    atts['colspan'] = node['morecols'] + 1
        #    node.parent.column += node['morecols']
        #self.body.append(self.starttag(node, tagname, '', **atts))
        #self.context.append('</%s>\n' % tagname.lower())
        #if len(node) == 0:              # empty cell
        #    self.body.append('&nbsp;')
        #self.set_first_last(node)
        print node

    def depart_entry(self, node):
        #self.body.append(self.context.pop())
        print node

    def visit_enumerated_list(self, node):
        """
        The 'start' attribute does not conform to HTML 4.01's strict.dtd, but
        CSS1 doesn't help. CSS2 isn't widely enough supported yet to be
        usable.
        """
        atts = {}
        if 'start' in node:
            atts['start'] = node['start']
        if 'enumtype' in node:
            atts['class'] = node['enumtype']
        # @@@ To do: prefix, suffix. How? Change prefix/suffix to a
        # single "format" attribute? Use CSS2?
        old_compact_simple = self.compact_simple
        self.context.append((self.compact_simple, self.compact_p))
        self.compact_p = None
        if self.compact_simple and not old_compact_simple:
            atts['class'] = (atts.get('class', '') + ' simple').strip()
        self.body.append(self.starttag(node, 'ol', **atts))

    def depart_enumerated_list(self, node):
        self.compact_simple, self.compact_p = self.context.pop()
        self.body.append('</ol>\n')

    def visit_field(self, node):
        #self.body.append(self.starttag(node, 'tr', '', CLASS='field'))
        pass

    def depart_field(self, node):
        #self.body.append('</tr>\n')
        pass

    def visit_field_body(self, node):
        #self.body.append(self.starttag(node, 'td', '', CLASS='field-body'))
        #self.set_class_on_child(node, 'first', 0)
        #field = node.parent
        #if (self.compact_field_list or
        #    isinstance(field.parent, nodes.docinfo) or
        #    field.parent.index(field) == len(field.parent) - 1):
        #    # If we are in a compact list, the docinfo, or if this is
        #    # the last field of the field list, do not add vertical
        #    # space after last element.
        #    self.set_class_on_child(node, 'last', -1)
        self.body.append(': ')

    def depart_field_body(self, node):
        #self.body.append('</td>\n')
        pass

    def visit_field_list(self, node):
        #self.context.append((self.compact_field_list, self.compact_p))
        #self.compact_p = None
        #if 'compact' in node['classes']:
        #    self.compact_field_list = True
        #elif (self.settings.compact_field_lists
        #      and 'open' not in node['classes']):
        #    self.compact_field_list = True
        #if self.compact_field_list:
        #    for field in node:
        #        field_body = field[-1]
        #        assert isinstance(field_body, nodes.field_body)
        #        children = [n for n in field_body
        #                    if not isinstance(n, nodes.Invisible)]
        #        if not (len(children) == 0 or
        #                len(children) == 1 and
        #                isinstance(children[0],
        #                           (nodes.paragraph, nodes.line_block))):
        #            self.compact_field_list = False
        #            break
        #self.body.append(self.starttag(node, 'table', frame='void',
        #                               rules='none',
        #                               CLASS='docutils field-list'))
        #self.body.append('<col class="field-name" />\n'
        #                 '<col class="field-body" />\n'
        #                 '<tbody valign="top">\n')
        pass

    def depart_field_list(self, node):
        #self.body.append('</tbody>\n</table>\n')
        #self.compact_field_list, self.compact_p = self.context.pop()
        pass

    def visit_field_name(self, node):
        #atts = {}
        #if self.in_docinfo:
        #    atts['class'] = 'docinfo-name'
        #else:
        #    atts['class'] = 'field-name'
        #if ( self.settings.field_name_limit
        #     and len(node.astext()) > self.settings.field_name_limit):
        #    atts['colspan'] = 2
        #    self.context.append('</tr>\n'
        #                        + self.starttag(node.parent, 'tr', '', 
        #                                        CLASS='field')
        #                        + '<td>&nbsp;</td>')
        #else:
        #    self.context.append('')
        #self.body.append(self.starttag(node, 'th', '', **atts))
        self.body.append('* ')

    def depart_field_name(self, node):
        #self.body.append(':</th>')
        #self.body.append(self.context.pop())
        pass

    def visit_figure(self, node):
        #atts = {'class': 'figure'}
        #if node.get('width'):
        #    atts['style'] = 'width: %s' % node['width']
        #if node.get('align'):
        #    atts['class'] += " align-" + node['align']
        #self.body.append(self.starttag(node, 'div', **atts))
        pass

    def depart_figure(self, node):
        #self.body.append('</div>\n')
        pass

    def visit_footer(self, node):
        self.context.append(len(self.body))

    def depart_footer(self, node):
        pass
        #start = self.context.pop()
        #footer = [self.starttag(node, 'div', CLASS='footer'),
        #          '<hr class="footer" />\n']
        #footer.extend(self.body[start:])
        #footer.append('\n</div>\n')
        #self.footer.extend(footer)
        #self.body_suffix[:0] = footer
        #del self.body[start:]

    def visit_footnote(self, node):
        self.body.append(self.starttag(node, 'table',
                                       CLASS='docutils footnote',
                                       frame="void", rules="none"))
        self.body.append('<colgroup><col class="label" /><col /></colgroup>\n'
                         '<tbody valign="top">\n'
                         '<tr>')
        self.footnote_backrefs(node)

    def footnote_backrefs(self, node):
        backlinks = []
        backrefs = node['backrefs']
        if self.settings.footnote_backlinks and backrefs:
            if len(backrefs) == 1:
                self.context.append('')
                self.context.append('</a>')
                self.context.append('<a class="fn-backref" href="#%s">'
                                    % backrefs[0])
            else:
                i = 1
                for backref in backrefs:
                    backlinks.append('<a class="fn-backref" href="#%s">%s</a>'
                                     % (backref, i))
                    i += 1
                self.context.append('<em>(%s)</em> ' % ', '.join(backlinks))
                self.context += ['', '']
        else:
            self.context.append('')
            self.context += ['', '']
        # If the node does not only consist of a label.
        if len(node) > 1:
            # If there are preceding backlinks, we do not set class
            # 'first', because we need to retain the top-margin.
            if not backlinks:
                node[1]['classes'].append('first')
            node[-1]['classes'].append('last')

    def depart_footnote(self, node):
        self.body.append('</td></tr>\n'
                         '</tbody>\n</table>\n')

    def visit_footnote_reference(self, node):
        href = '#' + node['refid']
        format = self.settings.footnote_references
        if format == 'brackets':
            suffix = '['
            self.context.append(']')
        else:
            assert format == 'superscript'
            suffix = '<sup>'
            self.context.append('</sup>')
        self.body.append(self.starttag(node, 'a', suffix,
                                       CLASS='footnote-reference', href=href))

    def depart_footnote_reference(self, node):
        self.body.append(self.context.pop() + '</a>')

    def visit_generated(self, node):
        pass

    def depart_generated(self, node):
        pass

    def visit_header(self, node):
        self.context.append(len(self.body))

    def depart_header(self, node):
        start = self.context.pop()
        header = [self.starttag(node, 'div', CLASS='header')]
        header.extend(self.body[start:])
        header.append('\n<hr class="header"/>\n</div>\n')
        #self.body_prefix.extend(header)
        self.header.extend(header)
        del self.body[start:]

    def visit_image(self, node):
        atts = {}
        uri = node['uri']
        # place SVG and SWF images in an <object> element
        types = {'.svg': 'image/svg+xml',
                 '.swf': 'application/x-shockwave-flash'}
        ext = os.path.splitext(uri)[1].lower()
        if ext in ('.svg', '.swf'):
            atts['data'] = uri
            atts['type'] = types[ext]
        else:
            atts['src'] = uri
            atts['alt'] = node.get('alt', uri)
        # image size
        if 'width' in node:
            atts['width'] = node['width']
        if 'height' in node:
            atts['height'] = node['height']
        if 'scale' in node:
            if (PIL and not ('width' in node and 'height' in node)
                and self.settings.file_insertion_enabled):
                imagepath = urllib.url2pathname(uri)
                try:
                    img = PIL.Image.open(
                            imagepath.encode(sys.getfilesystemencoding()))
                except (IOError, UnicodeEncodeError):
                    pass # TODO: warn?
                else:
                    self.settings.record_dependencies.add(
                        imagepath.replace('\\', '/'))
                    if 'width' not in atts:
                        atts['width'] = '%dpx' % img.size[0]
                    if 'height' not in atts:
                        atts['height'] = '%dpx' % img.size[1]
                    del img
            for att_name in 'width', 'height':
                if att_name in atts:
                    match = re.match(r'([0-9.]+)(\S*)$', atts[att_name])
                    assert match
                    atts[att_name] = '%s%s' % (
                        float(match.group(1)) * (float(node['scale']) / 100),
                        match.group(2))
        style = []
        for att_name in 'width', 'height':
            if att_name in atts:
                if re.match(r'^[0-9.]+$', atts[att_name]):
                    # Interpret unitless values as pixels.
                    atts[att_name] += 'px'
                style.append('%s: %s;' % (att_name, atts[att_name]))
                del atts[att_name]
        if style:
            atts['style'] = ' '.join(style)
        if (isinstance(node.parent, nodes.TextElement) or
            (isinstance(node.parent, nodes.reference) and
             not isinstance(node.parent.parent, nodes.TextElement))):
            # Inline context or surrounded by <a>...</a>.
            suffix = ''
        else:
            suffix = '\n'
        if 'align' in node:
            atts['class'] = 'align-%s' % node['align']
        self.context.append('')
        if ext in ('.svg', '.swf'): # place in an object element,
            # do NOT use an empty tag: incorrect rendering in browsers
            self.body.append(self.starttag(node, 'object', suffix, **atts) +
                             node.get('alt', uri) + '</object>' + suffix)
        else:
            self.body.append(self.emptytag(node, 'img', suffix, **atts))

    def depart_image(self, node):
        self.body.append(self.context.pop())

    def visit_inline(self, node):
        #self.body.append(self.starttag(node, 'span', ''))
        pass

    def depart_inline(self, node):
        #self.body.append('</span>')
        pass

    def visit_label(self, node):
        # Context added in footnote_backrefs.
        self.body.append(self.starttag(node, 'td', '%s[' % self.context.pop(),
                                       CLASS='label'))

    def depart_label(self, node):
        # Context added in footnote_backrefs.
        self.body.append(']%s</td><td>%s' % (self.context.pop(), self.context.pop()))

    def visit_legend(self, node):
        self.body.append(self.starttag(node, 'div', CLASS='legend'))

    def depart_legend(self, node):
        self.body.append('</div>\n')

    def visit_line(self, node):
        self.body.append(self.starttag(node, 'div', suffix='', CLASS='line'))
        if not len(node):
            self.body.append('<br />')

    def depart_line(self, node):
        self.body.append('</div>\n')

    def visit_line_block(self, node):
        self.body.append(self.starttag(node, 'div', CLASS='line-block'))

    def depart_line_block(self, node):
        self.body.append('</div>\n')

    def visit_list_item(self, node):
        list_char = '-' if self.list_level % 2 else '*'
        self.body.append(('    ' * (self.list_level - 1)) + list_char + ' ')

    def depart_list_item(self, node):
        #self.body.append('\n')
        pass

    def visit_literal(self, node):
        self.protect_literal_text += 1

    def depart_literal(self, node):
        # skipped unless literal element is from "code" role:
        self.protect_literal_text -= 1

    def visit_literal_block(self, node):
        self.protect_literal_text += 1

    def depart_literal_block(self, node):
        self.protect_literal_text -= 1
        self.body.append('\n\n')

    def visit_math(self, node, math_env=''):
        pass

    def depart_math(self, node):
        pass # never reached

    def visit_math_block(self, node):
        # print node.astext().encode('utf8')
        math_env = pick_math_environment(node.astext())
        self.visit_math(node, math_env=math_env)

    def depart_math_block(self, node):
        pass # never reached

    def visit_meta(self, node):
        pass

    def depart_meta(self, node):
        pass

    def visit_option(self, node):
        if self.context[-1]:
            self.body.append(', ')
        self.body.append(self.starttag(node, 'span', '', CLASS='option'))

    def depart_option(self, node):
        self.body.append('</span>')
        self.context[-1] += 1

    def visit_option_argument(self, node):
        self.body.append(node.get('delimiter', ' '))
        self.body.append(self.starttag(node, 'var', ''))

    def depart_option_argument(self, node):
        self.body.append('</var>')

    def visit_option_group(self, node):
        atts = {}
        if ( self.settings.option_limit
             and len(node.astext()) > self.settings.option_limit):
            atts['colspan'] = 2
            self.context.append('</tr>\n<tr><td>&nbsp;</td>')
        else:
            self.context.append('')
        self.body.append(
            self.starttag(node, 'td', CLASS='option-group', **atts))
        self.body.append('<kbd>')
        self.context.append(0)          # count number of options

    def depart_option_group(self, node):
        self.context.pop()
        self.body.append('</kbd></td>\n')
        self.body.append(self.context.pop())

    def visit_option_list(self, node):
        pass

    def depart_option_list(self, node):
        self.body.append('\n')

    def visit_option_list_item(self, node):
        #self.body.append('* ')
        pass

    def depart_option_list_item(self, node):
        #self.body.append('\n')
        pass

    def visit_option_string(self, node):
        pass

    def depart_option_string(self, node):
        pass

    def visit_organization(self, node):
        self.visit_docinfo_item(node, 'organization')

    def depart_organization(self, node):
        self.depart_docinfo_item()

    def visit_paragraph(self, node):
        pass

    def depart_paragraph(self, node):
        self.body.append('\n\n')

    def visit_problematic(self, node):
        if node.hasattr('refid'):
            self.body.append('<a href="#%s">' % node['refid'])
            self.context.append('</a>')
        else:
            self.context.append('')
        self.body.append(self.starttag(node, 'span', '', CLASS='problematic'))

    def depart_problematic(self, node):
        self.body.append('</span>')
        self.body.append(self.context.pop())

    def visit_raw(self, node):
        if 'html' in node.get('format', '').split():
            t = isinstance(node.parent, nodes.TextElement) and 'span' or 'div'
            if node['classes']:
                self.body.append(self.starttag(node, t, suffix=''))
            self.body.append(node.astext())
            if node['classes']:
                self.body.append('</%s>' % t)
        # Keep non-HTML raw text out of output:
        raise nodes.SkipNode

    # overwritten
    def visit_reference(self, node):
        self.body.append('[')

    def depart_reference(self, node):
        #if node.get('secnumber'):
        #    self.body.append(('%s' + self.secnumber_suffix) %
        #                     '.'.join(map(str, node['secnumber'])))
        #if node.get('internal') or 'refuri' not in node:
        #    atts['class'] += ' internal'
        #else:
        #    atts['class'] += ' external'

        if 'refuri' in node:
            href = node['refuri'] or '#'
        else:
            assert 'refid' in node, \
                   'References must have "refuri" or "refid" attribute.'
            href = '#' + node['refid']

        reftitle = node.get('reftitle')

        self.body.append(']({href}{title})'.format(
            href=href,
            title=' "{}"'.format(reftitle) if reftitle else '',
        ))

    def visit_revision(self, node):
        self.visit_docinfo_item(node, 'revision', meta=False)

    def depart_revision(self, node):
        self.depart_docinfo_item()

    def visit_row(self, node):
        #self.body.append(self.starttag(node, 'tr', ''))
        node.column = 0

    def depart_row(self, node):
        self.body.append('\n')

    def visit_rubric(self, node):
        self.body.append(self.starttag(node, 'p', '', CLASS='rubric'))

    def depart_rubric(self, node):
        self.body.append('</p>\n')

    def visit_section(self, node):
        self.section_level += 1

    def depart_section(self, node):
        self.section_level -= 1
        #self.body.append('\n')

    def visit_sidebar(self, node):
        self.body.append(
            self.starttag(node, 'div', CLASS='sidebar'))
        self.set_first_last(node)
        self.in_sidebar = True

    def depart_sidebar(self, node):
        self.body.append('</div>\n')
        self.in_sidebar = False

    def visit_status(self, node):
        self.visit_docinfo_item(node, 'status', meta=False)

    def depart_status(self, node):
        self.depart_docinfo_item()

    def visit_strong(self, node):
        self.body.append(self.starttag(node, 'strong', ''))

    def depart_strong(self, node):
        self.body.append('</strong>')

    def visit_subscript(self, node):
        self.body.append(self.starttag(node, 'sub', ''))

    def depart_subscript(self, node):
        self.body.append('</sub>')

    def visit_substitution_definition(self, node):
        """Internal only."""
        raise nodes.SkipNode

    def visit_substitution_reference(self, node):
        self.unimplemented_visit(node)

    def visit_subtitle(self, node):
        if isinstance(node.parent, nodes.sidebar):
            self.body.append(self.starttag(node, 'p', '',
                                           CLASS='sidebar-subtitle'))
            self.context.append('</p>\n')
        elif isinstance(node.parent, nodes.document):
            self.body.append(self.starttag(node, 'h2', '', CLASS='subtitle'))
            self.context.append('</h2>\n')
            self.in_document_title = len(self.body)
        elif isinstance(node.parent, nodes.section):
            tag = 'h%s' % (self.section_level + self.initial_header_level - 1)
            self.body.append(
                self.starttag(node, tag, '', CLASS='section-subtitle') +
                self.starttag({}, 'span', '', CLASS='section-subtitle'))
            self.context.append('</span></%s>\n' % tag)

    def depart_subtitle(self, node):
        self.body.append(self.context.pop())
        if self.in_document_title:
            self.subtitle = self.body[self.in_document_title:-1]
            self.in_document_title = 0
            self.body_pre_docinfo.extend(self.body)
            del self.body[:]

    def visit_superscript(self, node):
        self.body.append(self.starttag(node, 'sup', ''))

    def depart_superscript(self, node):
        self.body.append('</sup>')

    def visit_system_message(self, node):
        self.body.append(self.starttag(node, 'div', CLASS='system-message'))
        self.body.append('<p class="system-message-title">')
        backref_text = ''
        if len(node['backrefs']):
            backrefs = node['backrefs']
            if len(backrefs) == 1:
                backref_text = ('; <em><a href="#%s">backlink</a></em>'
                                % backrefs[0])
            else:
                i = 1
                backlinks = []
                for backref in backrefs:
                    backlinks.append('<a href="#%s">%s</a>' % (backref, i))
                    i += 1
                backref_text = ('; <em>backlinks: %s</em>'
                                % ', '.join(backlinks))
        if node.hasattr('line'):
            line = ', line %s' % node['line']
        else:
            line = ''
        self.body.append('System Message: %s/%s '
                         '(<tt class="docutils">%s</tt>%s)%s</p>\n'
                         % (node['type'], node['level'],
                            self.encode(node['source']), line, backref_text))

    def depart_system_message(self, node):
        self.body.append('</div>\n')

    def visit_table(self, node):
        #self.context.append(self.compact_p)
        #self.compact_p = True
        #classes = ' '.join(['docutils', self.settings.table_style]).strip()
        #self.body.append(
        #    self.starttag(node, 'table', CLASS=classes, border="1"))
        print node

    def depart_table(self, node):
        #self.compact_p = self.context.pop()
        #self.body.append('</table>\n')
        print node

    def visit_target(self, node):
        if not ('refuri' in node or 'refid' in node
                or 'refname' in node):
            self.body.append(self.starttag(node, 'span', '', CLASS='target'))
            self.context.append('</span>')
        else:
            self.context.append('')

    def depart_target(self, node):
        self.body.append(self.context.pop())

    def visit_tbody(self, node):
        #self.write_colspecs()
        #self.body.append(self.context.pop()) # '</colgroup>\n' or ''
        #self.body.append(self.starttag(node, 'tbody', valign='top'))
        pass

    def depart_tbody(self, node):
        #self.body.append('</tbody>\n')
        pass

    def visit_term(self, node):
        self.body.append(self.starttag(node, 'dt', ''))

    def depart_term(self, node):
        """
        Leave the end tag to `self.visit_definition()`, in case there's a
        classifier.
        """
        pass

    def visit_tgroup(self, node):
        # Mozilla needs <colgroup>:
        self.body.append(self.starttag(node, 'colgroup'))
        # Appended by thead or tbody:
        self.context.append('</colgroup>\n')
        node.stubs = []

    def depart_tgroup(self, node):
        pass

    def visit_thead(self, node):
        #self.write_colspecs()
        #self.body.append(self.context.pop()) # '</colgroup>\n'
        # There may or may not be a <thead>; this is for <tbody> to use:
        #self.context.append('')
        #self.body.append(self.starttag(node, 'thead', valign='bottom'))
        pass

    def depart_thead(self, node):
        #self.body.append('</thead>\n')
        pass

    def visit_title(self, node):
        """Only 6 section levels are supported by HTML."""
        check_id = 0  # TODO: is this a bool (False) or a counter?
        if isinstance(node.parent, nodes.topic):
            #self.body.append(
            #      self.starttag(node, 'p', '', CLASS='topic-title first'))
            pass
        elif isinstance(node.parent, nodes.sidebar):
            #self.body.append(
            #      self.starttag(node, 'p', '', CLASS='sidebar-title'))
            pass
        elif isinstance(node.parent, nodes.Admonition):
            #self.body.append(
            #      self.starttag(node, 'p', '', CLASS='admonition-title'))
            pass
        elif isinstance(node.parent, nodes.table):
            #self.body.append(
            #      self.starttag(node, 'caption', ''))
            #close_tag = '</caption>\n'
            pass
        elif isinstance(node.parent, nodes.document):
            #self.body.append(self.starttag(node, 'h1', '', CLASS='title'))
            #close_tag = '</h1>\n'
            #self.in_document_title = len(self.body)
            self.body.append('\n# ')
        else:
            assert isinstance(node.parent, nodes.section)
            h_level = self.section_level + self.initial_header_level - 1
            self.body.append('\n' + ('#' * h_level) + ' ')

            ids = node.parent.get('ids', [])
            for id_ in ids:
                self.body.append('<a name="{id}"></a>'.format(id=id_))
            #atts = {}
            #if (len(node.parent) >= 2 and
            #    isinstance(node.parent[1], nodes.subtitle)):
            #    atts['CLASS'] = 'with-subtitle'
            #self.body.append(
            #      self.starttag(node, 'h%s' % h_level, '', **atts))
            #atts = {}
            #if node.hasattr('refid'):
            #    atts['class'] = 'toc-backref'
            #    atts['href'] = '#' + node['refid']
            #if atts:
            #    self.body.append(self.starttag({}, 'a', '', **atts))
            #    close_tag = '</a></h%s>\n' % (h_level)
            #else:
            #    close_tag = '</h%s>\n' % (h_level)

        #self.context.append(close_tag)
        # TODO
        #self.add_secnumber(node)
        #self.add_fignumber(node.parent)

    def depart_title(self, node):
        self.body.append('\n\n')
        #self.body.append(self.context.pop())
        #if self.in_document_title:
        #    self.title = self.body[self.in_document_title:-1]
        #    self.in_document_title = 0
        #    self.body_pre_docinfo.extend(self.body)
        #    del self.body[:]

    def visit_title_reference(self, node):
        self.body.append(self.starttag(node, 'cite', ''))

    def depart_title_reference(self, node):
        self.body.append('</cite>')

    def visit_topic(self, node):
        #self.body.append(self.starttag(node, 'div', CLASS='topic'))
        self.topic_classes = node['classes']

    def depart_topic(self, node):
        #self.body.append('</div>\n')
        self.topic_classes = []

    def visit_transition(self, node):
        self.body.append(self.emptytag(node, 'hr', CLASS='docutils'))

    def depart_transition(self, node):
        pass

    def visit_version(self, node):
        self.visit_docinfo_item(node, 'version', meta=False)

    def depart_version(self, node):
        self.depart_docinfo_item()

    def unimplemented_visit(self, node):
        raise NotImplementedError('visiting unimplemented node type: %s'
                                  % node.__class__.__name__)


class SimpleListChecker(nodes.GenericNodeVisitor):

    """
    Raise `nodes.NodeFound` if non-simple list item is encountered.

    Here "simple" means a list item containing nothing other than a single
    paragraph, a simple list, or a paragraph followed by a simple list.
    """

    def default_visit(self, node):
        raise nodes.NodeFound

    def visit_bullet_list(self, node):
        pass

    def visit_enumerated_list(self, node):
        pass

    def visit_list_item(self, node):
        children = []
        for child in node.children:
            if not isinstance(child, nodes.Invisible):
                children.append(child)
        if (children and isinstance(children[0], nodes.paragraph)
            and (isinstance(children[-1], nodes.bullet_list)
                 or isinstance(children[-1], nodes.enumerated_list))):
            children.pop()
        if len(children) <= 1:
            return
        else:
            raise nodes.NodeFound

    def visit_paragraph(self, node):
        raise nodes.SkipNode

    def invisible_visit(self, node):
        """Invisible nodes should be ignored."""
        raise nodes.SkipNode

    visit_comment = invisible_visit
    visit_substitution_definition = invisible_visit
    visit_target = invisible_visit
    visit_pending = invisible_visit
