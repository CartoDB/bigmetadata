import codecs
import copy
import posixpath
import sys
import warnings
import zlib

from docutils import nodes
from docutils.io import DocTreeInput, StringOutput
from docutils.core import Publisher
from docutils.utils import new_document, relative_path
from docutils.frontend import OptionParser
from docutils.readers.doctree import Reader as DoctreeReader

from sphinx import package_dir, __display_version__, addnodes
from sphinx.builders import Builder
from sphinx.builders.html import get_stable_hash
from sphinx.util import jsonimpl, copy_static_entry, copy_extra_entry
from sphinx.util.images import get_image_size
from sphinx.util.i18n import format_date
from sphinx.util.osutil import SEP, os_path, relative_uri, ensuredir, \
            movefile, copyfile
from sphinx.util.nodes import inline_all_toctrees
from sphinx.util.matching import patmatch, compile_matchers
from sphinx.locale import admonitionlabels, _
from sphinx.search import js_index
from sphinx.theming import Theme
from sphinx.builders import Builder
from sphinx.application import ENV_PICKLE_FILENAME
from sphinx.util.console import bold, darkgreen, brown

from six import iteritems, text_type, string_types
from six.moves import cPickle as pickle
from os import path

from markdown_writer import Writer, MarkdownTranslator as BaseTranslator

#: the filename for the inventory of objects
INVENTORY_FILENAME = 'objects.inv'
#: the filename for the "last build" file (for serializing builders)
LAST_BUILD_FILENAME = 'last_build'


class MarkdownWriter(Writer):

    def __init__(self, builder):
        Writer.__init__(self)
        self.builder = builder

    def translate(self):
        # sadly, this is mostly copied from parent class
        self.visitor = visitor = self.builder.translator_class(self.builder,
                                                               self.document)
        self.document.walkabout(visitor)
        self.output = visitor.astext()
        for attr in (
                     'body_pre_docinfo', 'docinfo', 'body', 'fragment',
                     'body_suffix', 'meta', 'title', 'subtitle', 'header',
                     'footer', ):
            setattr(self, attr, getattr(visitor, attr, None))


class MarkdownTranslator(BaseTranslator):
    """
    Our custom HTML translator.
    """

    def __init__(self, builder, *args, **kwds):
        BaseTranslator.__init__(self, *args, **kwds)
    #    self.no_smarty = 0
        self.builder = builder
    #    self.protect_literal_text = 0
        self.permalink_text = builder.config.html_add_permalinks
    #    # support backwards-compatible setting to a bool
        if not isinstance(self.permalink_text, string_types):
            self.permalink_text = self.permalink_text and u'\u00B6' or ''
    #    self.permalink_text = self.encode(self.permalink_text)
    #    self.secnumber_suffix = builder.config.html_secnumber_suffix
    #    self.param_separator = ''
    #    self.optional_param_level = 0
    #    self._table_row_index = 0

    def visit_start_of_file(self, node):
        pass

    def depart_start_of_file(self, node):
        pass

    #def visit_desc(self, node):
    #    self.body.append(self.starttag(node, 'dl', CLASS=node['objtype']))

    #def depart_desc(self, node):
    #    self.body.append('</dl>\n\n')

    #def visit_desc_signature(self, node):
    #    # the id is set automatically
    #    self.body.append(self.starttag(node, 'dt'))
    #    # anchor for per-desc interactive data
    #    if node.parent['objtype'] != 'describe' \
    #       and node['ids'] and node['first']:
    #        self.body.append('<!--[%s]-->' % node['ids'][0])

    #def depart_desc_signature(self, node):
    #    self.add_permalink_ref(node, _('Permalink to this definition'))
    #    self.body.append('</dt>\n')

    #def visit_desc_addname(self, node):
    #    self.body.append(self.starttag(node, 'code', '', CLASS='descclassname'))

    #def depart_desc_addname(self, node):
    #    self.body.append('</code>')

    #def visit_desc_type(self, node):
    #    pass

    #def depart_desc_type(self, node):
    #    pass

    #def visit_desc_returns(self, node):
    #    self.body.append(' &rarr; ')

    #def depart_desc_returns(self, node):
    #    pass

    #def visit_desc_name(self, node):
    #    self.body.append(self.starttag(node, 'code', '', CLASS='descname'))

    #def depart_desc_name(self, node):
    #    self.body.append('</code>')

    #def visit_desc_parameterlist(self, node):
    #    self.body.append('<span class="sig-paren">(</span>')
    #    self.first_param = 1
    #    self.optional_param_level = 0
    #    # How many required parameters are left.
    #    self.required_params_left = sum([isinstance(c, addnodes.desc_parameter)
    #                                     for c in node.children])
    #    self.param_separator = node.child_text_separator

    #def depart_desc_parameterlist(self, node):
    #    self.body.append('<span class="sig-paren">)</span>')

    ## If required parameters are still to come, then put the comma after
    ## the parameter.  Otherwise, put the comma before.  This ensures that
    ## signatures like the following render correctly (see issue #1001):
    ##
    ##     foo([a, ]b, c[, d])
    ##
    #def visit_desc_parameter(self, node):
    #    if self.first_param:
    #        self.first_param = 0
    #    elif not self.required_params_left:
    #        self.body.append(self.param_separator)
    #    if self.optional_param_level == 0:
    #        self.required_params_left -= 1
    #    if not node.hasattr('noemph'):
    #        self.body.append('<em>')

    #def depart_desc_parameter(self, node):
    #    if not node.hasattr('noemph'):
    #        self.body.append('</em>')
    #    if self.required_params_left:
    #        self.body.append(self.param_separator)

    #def visit_desc_optional(self, node):
    #    self.optional_param_level += 1
    #    self.body.append('<span class="optional">[</span>')

    #def depart_desc_optional(self, node):
    #    self.optional_param_level -= 1
    #    self.body.append('<span class="optional">]</span>')

    #def visit_desc_annotation(self, node):
    #    self.body.append(self.starttag(node, 'em', '', CLASS='property'))

    #def depart_desc_annotation(self, node):
    #    self.body.append('</em>')

    #def visit_desc_content(self, node):
    #    self.body.append(self.starttag(node, 'dd', ''))

    #def depart_desc_content(self, node):
    #    self.body.append('</dd>')

    def visit_versionmodified(self, node):
        pass

    def depart_versionmodified(self, node):
        self.body.append('\n\n')


    def visit_number_reference(self, node):
        self.visit_reference(node)

    def depart_number_reference(self, node):
        self.depart_reference(node)

    # overwritten -- we don't want source comments to show up in the HTML
    def visit_comment(self, node):
        raise nodes.SkipNode

    # overwritten
    def visit_admonition(self, node, name=''):
        self.body.append(self.starttag(
            node, 'div', CLASS=('admonition ' + name)))
        if name:
            node.insert(0, nodes.title(name, admonitionlabels[name]))
        self.set_first_last(node)

    def visit_seealso(self, node):
        self.visit_admonition(node, 'seealso')

    def depart_seealso(self, node):
        self.depart_admonition(node)

    def add_secnumber(self, node):
        if node.get('secnumber'):
            self.body.append('.'.join(map(str, node['secnumber'])) +
                             self.secnumber_suffix)
        #elif isinstance(node.parent, nodes.section):
        #    if self.builder.name == 'singlehtml':
        #        docname = node.parent.get('docname')
        #        anchorname = '#' + node.parent['ids'][0]
        #        if (docname, anchorname) not in self.builder.secnumbers:
        #            anchorname = (docname, '')  # try first heading which has no anchor
        #        else:
        #            anchorname = (docname, anchorname)
        #    else:
        #        anchorname = '#' + node.parent['ids'][0]
        #        if anchorname not in self.builder.secnumbers:
        #            anchorname = ''  # try first heading which has no anchor
        #    if self.builder.secnumbers.get(anchorname):
        #        numbers = self.builder.secnumbers[anchorname]
        #        self.body.append('.'.join(map(str, numbers)) +
        #                         self.secnumber_suffix)

    #def add_fignumber(self, node):
    #    def append_fignumber(figtype, figure_id):
    #        if figure_id in self.builder.fignumbers.get(figtype, {}):
    #            self.body.append('<span class="caption-number">')
    #            prefix = self.builder.config.numfig_format.get(figtype)
    #            if prefix is None:
    #                msg = 'numfig_format is not defined for %s' % figtype
    #                self.builder.warn(msg)
    #            else:
    #                numbers = self.builder.fignumbers[figtype][figure_id]
    #                self.body.append(prefix % '.'.join(map(str, numbers)) + ' ')
    #                self.body.append('</span>')

    #    figtype = self.builder.env.domains['std'].get_figtype(node)
    #    if figtype:
    #        if len(node['ids']) == 0:
    #            msg = 'Any IDs not assigned for %s node' % node.tagname
    #            self.builder.env.warn_node(msg, node)
    #        else:
    #            append_fignumber(figtype, node['ids'][0])

    def add_permalink_ref(self, node, title):
        if node['ids'] and self.permalink_text and self.builder.add_permalinks:
            format = u'<a class="headerlink" href="#%s" title="%s">%s</a>'
            self.body.append(format % (node['ids'][0], title, self.permalink_text))

    # overwritten to avoid emitting empty <ul></ul>
    def visit_bullet_list(self, node):
        if len(node) == 1 and node[0].tagname == 'toctree':
            raise nodes.SkipNode
        BaseTranslator.visit_bullet_list(self, node)

    def visit_caption(self, node):
        if isinstance(node.parent, nodes.container) and node.parent.get('literal_block'):
            self.body.append('<div class="code-block-caption">')
        else:
            BaseTranslator.visit_caption(self, node)
        self.add_fignumber(node.parent)
        self.body.append(self.starttag(node, 'span', '', CLASS='caption-text'))

    def depart_caption(self, node):
        self.body.append('</span>')

        # append permalink if available
        if isinstance(node.parent, nodes.container) and node.parent.get('literal_block'):
            self.add_permalink_ref(node.parent, _('Permalink to this code'))
        elif isinstance(node.parent, nodes.figure):
            image_nodes = node.parent.traverse(nodes.image)
            target_node = image_nodes and image_nodes[0] or node.parent
            self.add_permalink_ref(target_node, _('Permalink to this image'))
        elif node.parent.get('toctree'):
            self.add_permalink_ref(node.parent.parent, _('Permalink to this toctree'))

        if isinstance(node.parent, nodes.container) and node.parent.get('literal_block'):
            self.body.append('</div>\n')
        else:
            BaseTranslator.depart_caption(self, node)

    def visit_doctest_block(self, node):
        self.visit_literal_block(node)

    # overwritten to add the <div> (for XHTML compliance)
    def visit_block_quote(self, node):
        self.body.append(self.starttag(node, 'blockquote') + '<div>')

    def depart_block_quote(self, node):
        self.body.append('</div></blockquote>\n')

    def visit_productionlist(self, node):
        self.body.append(self.starttag(node, 'pre'))
        names = []
        for production in node:
            names.append(production['tokenname'])
        maxlen = max(len(name) for name in names)
        lastname = None
        for production in node:
            if production['tokenname']:
                lastname = production['tokenname'].ljust(maxlen)
                self.body.append(self.starttag(production, 'strong', ''))
                self.body.append(lastname + '</strong> ::= ')
            elif lastname is not None:
                self.body.append('%s     ' % (' '*len(lastname)))
            production.walkabout(self)
            self.body.append('\n')
        self.body.append('</pre>\n')
        raise nodes.SkipNode

    def depart_productionlist(self, node):
        pass

    def visit_production(self, node):
        pass

    def depart_production(self, node):
        pass

    def visit_centered(self, node):
        self.body.append(self.starttag(node, 'p', CLASS="centered") +
                         '<strong>')

    def depart_centered(self, node):
        self.body.append('</strong></p>')

    def visit_download_reference(self, node):
        if self.builder.download_support and node.hasattr('filename'):
            self.body.append(
                '<a class="reference download internal" href="%s" download="">' %
                posixpath.join(self.builder.dlpath, node['filename']))
            self.context.append('</a>')
        else:
            self.context.append('')

    def depart_download_reference(self, node):
        self.body.append(self.context.pop())

    # overwritten
    def visit_image(self, node):
        olduri = node['uri']
        # rewrite the URI if the environment knows about it
        if olduri in self.builder.images:
            node['uri'] = posixpath.join(self.builder.imgpath,
                                         self.builder.images[olduri])

        uri = node['uri']
        if uri.lower().endswith('svg') or uri.lower().endswith('svgz'):
            atts = {'src': uri}
            if 'width' in node:
                atts['width'] = node['width']
            if 'height' in node:
                atts['height'] = node['height']
            atts['alt'] = node.get('alt', uri)
            if 'align' in node:
                self.body.append('<div align="%s" class="align-%s">' %
                                 (node['align'], node['align']))
                self.context.append('</div>\n')
            else:
                self.context.append('')
            self.body.append(self.emptytag(node, 'img', '', **atts))
            return

        if 'scale' in node:
            # Try to figure out image height and width.  Docutils does that too,
            # but it tries the final file name, which does not necessarily exist
            # yet at the time the HTML file is written.
            if not ('width' in node and 'height' in node):
                size = get_image_size(path.join(self.builder.srcdir, olduri))
                if size is None:
                    self.builder.env.warn_node('Could not obtain image size. '
                                               ':scale: option is ignored.', node)
                else:
                    if 'width' not in node:
                        node['width'] = str(size[0])
                    if 'height' not in node:
                        node['height'] = str(size[1])
        BaseTranslator.visit_image(self, node)

    def visit_toctree(self, node):
        # this only happens when formatting a toc from env.tocs -- in this
        # case we don't want to include the subtree
        raise nodes.SkipNode

    def visit_index(self, node):
        raise nodes.SkipNode

    def visit_tabular_col_spec(self, node):
        raise nodes.SkipNode

    def visit_glossary(self, node):
        pass

    def depart_glossary(self, node):
        pass

    def visit_acks(self, node):
        pass

    def depart_acks(self, node):
        pass

    def visit_hlist(self, node):
        self.body.append('<table class="hlist"><tr>')

    def depart_hlist(self, node):
        self.body.append('</tr></table>\n')

    def visit_hlistcol(self, node):
        self.body.append('<td>')

    def depart_hlistcol(self, node):
        self.body.append('</td>')

    def visit_note(self, node):
        self.visit_admonition(node, 'note')

    def depart_note(self, node):
        self.depart_admonition(node)

    def visit_warning(self, node):
        self.visit_admonition(node, 'warning')

    def depart_warning(self, node):
        self.depart_admonition(node)

    def visit_attention(self, node):
        self.visit_admonition(node, 'attention')

    def depart_attention(self, node):
        self.depart_admonition()

    def visit_caution(self, node):
        self.visit_admonition(node, 'caution')

    def depart_caution(self, node):
        self.depart_admonition()

    def visit_danger(self, node):
        self.visit_admonition(node, 'danger')

    def depart_danger(self, node):
        self.depart_admonition()

    def visit_error(self, node):
        self.visit_admonition(node, 'error')

    def depart_error(self, node):
        self.depart_admonition()

    def visit_hint(self, node):
        self.visit_admonition(node, 'hint')

    def depart_hint(self, node):
        self.depart_admonition()

    def visit_important(self, node):
        self.visit_admonition(node, 'important')

    def depart_important(self, node):
        self.depart_admonition()

    def visit_tip(self, node):
        self.visit_admonition(node, 'tip')

    def depart_tip(self, node):
        self.depart_admonition()

    # these are only handled specially in the SmartyPantsHTMLTranslator
    def visit_literal_emphasis(self, node):
        return self.visit_emphasis(node)

    def depart_literal_emphasis(self, node):
        return self.depart_emphasis(node)

    def visit_literal_strong(self, node):
        return self.visit_strong(node)

    def depart_literal_strong(self, node):
        return self.depart_strong(node)

    def visit_abbreviation(self, node):
        attrs = {}
        if node.hasattr('explanation'):
            attrs['title'] = node['explanation']
        self.body.append(self.starttag(node, 'abbr', '', **attrs))

    def depart_abbreviation(self, node):
        self.body.append('</abbr>')

    # overwritten (but not changed) to keep pair of visit/depart_term
    def visit_term(self, node):
        self.body.append(self.starttag(node, 'dt', ''))

    # overwritten to add '</dt>' in 'depart_term' state.
    def depart_term(self, node):
        self.body.append('</dt>\n')

    # overwritten to do not add '</dt>' in 'visit_definition' state.
    def visit_definition(self, node):
        self.body.append(self.starttag(node, 'dd', ''))
        self.set_first_last(node)

    # overwritten (but not changed) to keep pair of visit/depart_definition
    def depart_definition(self, node):
        self.body.append('</dd>\n')

    def visit_termsep(self, node):
        warnings.warn('sphinx.addnodes.termsep will be removed at Sphinx-1.5',
                      DeprecationWarning)
        self.body.append('<br />')
        raise nodes.SkipNode

    def visit_manpage(self, node):
        return self.visit_literal_emphasis(node)

    def depart_manpage(self, node):
        return self.depart_literal_emphasis(node)

    # overwritten to add even/odd classes

    def visit_math(self, node, math_env=''):
        self.builder.warn('using "math" markup without a Sphinx math extension '
                          'active, please use one of the math extensions '
                          'described at http://sphinx-doc.org/ext/math.html',
                          (self.builder.current_docname, node.line))
        raise nodes.SkipNode

    def unknown_visit(self, node):
        raise NotImplementedError('Unknown node: ' + node.__class__.__name__)


class MarkdownBuilder(Builder):
    """
    Builds standalone Markdown docs.
    """
    name = 'markdown'
    format = 'markdown'
    copysource = True
    allow_parallel = True
    out_suffix = '.md'
    link_suffix = '.md'  # defaults to matching out_suffix
    #indexer_format = js_index
    indexer_dumps_unicode = True
    supported_image_types = ['image/svg+xml', 'image/png',
                             'image/gif', 'image/jpeg']
    #searchindex_filename = 'searchindex.js'
    add_permalinks = True
    embedded = False  # for things like HTML help or Qt help: suppresses sidebar
    search = False  # for things like HTML help and Apple help: suppress search

    # This is a class attribute because it is mutated by Sphinx.add_javascript.
    #script_files = ['_static/jquery.js', '_static/underscore.js',
    #                '_static/doctools.js']
    # Dito for this one.
    #css_files = []

    #default_sidebars = ['localtoc.html', 'relations.html',
    #                    'sourcelink.html', 'searchbox.html']

    # cached publisher object for snippets
    _publisher = None

    def init(self):
        # a hash of all config values that, if changed, cause a full rebuild
        self.config_hash = ''
        self.tags_hash = ''
        # basename of images directory
        self.imagedir = '_images'
        # section numbers for headings in the currently visited document
        self.secnumbers = {}
        # currently written docname
        self.current_docname = None
        self.translator_class = MarkdownTranslator

        if self.config.html_file_suffix is not None:
            self.out_suffix = self.config.html_file_suffix

        if self.config.html_link_suffix is not None:
            self.link_suffix = self.config.html_link_suffix
        else:
            self.link_suffix = self.out_suffix

        self.create_template_bridge()
        self.templates.init(self, dirs='./')


    def get_target_uri(self, docname, typ=None):
        if docname == 'index':
            return ''
        if docname.endswith(SEP + 'index'):
            return docname[:-5]  # up to sep
        return docname + SEP

    def get_outfilename(self, pagename):
        if pagename == 'index' or pagename.endswith(SEP + 'index'):
            outfilename = path.join(self.outdir, os_path(pagename) +
                                    self.out_suffix)
        else:
            outfilename = path.join(self.outdir, os_path(pagename),
                                    'index' + self.out_suffix)

        return outfilename
    #def _get_translations_js(self):
    #    candidates = [path.join(package_dir, 'locale', self.config.language,
    #                            'LC_MESSAGES', 'sphinx.js'),
    #                  path.join(sys.prefix, 'share/sphinx/locale',
    #                            self.config.language, 'sphinx.js')] + \
    #                 [path.join(dir, self.config.language,
    #                            'LC_MESSAGES', 'sphinx.js')
    #                  for dir in self.config.locale_dirs]
    #    for jsfile in candidates:
    #        if path.isfile(jsfile):
    #            return jsfile
    #    return None

    #def init_translator_class(self):
    #    if self.translator_class is not None:
    #        pass
    #    elif self.config.html_translator_class:
    #        self.translator_class = self.app.import_object(
    #            self.config.html_translator_class,
    #            'html_translator_class setting')
    #    elif self.config.html_use_smartypants:
    #        self.translator_class = SmartyPantsHTMLTranslator
    #    else:
    #       self.translator_class = HTMLTranslator

    def get_outdated_docs(self):
        cfgdict = dict((name, self.config[name])
                       for (name, desc) in iteritems(self.config.values)
                       if desc[1] == 'html')
        self.config_hash = get_stable_hash(cfgdict)
        self.tags_hash = get_stable_hash(sorted(self.tags))
        old_config_hash = old_tags_hash = ''
        try:
            fp = open(path.join(self.outdir, '.buildinfo'))
            try:
                version = fp.readline()
                if version.rstrip() != '# Sphinx build info version 1':
                    raise ValueError
                fp.readline()  # skip commentary
                cfg, old_config_hash = fp.readline().strip().split(': ')
                if cfg != 'config':
                    raise ValueError
                tag, old_tags_hash = fp.readline().strip().split(': ')
                if tag != 'tags':
                    raise ValueError
            finally:
                fp.close()
        except ValueError:
            self.warn('unsupported build info format in %r, building all' %
                      path.join(self.outdir, '.buildinfo'))
        except Exception:
            pass
        if old_config_hash != self.config_hash or \
           old_tags_hash != self.tags_hash:

            for docname in self.env.found_docs:
                yield docname
            return

        if self.templates:
            template_mtime = self.templates.newest_template_mtime()
        else:
            template_mtime = 0

        for docname in self.env.found_docs:
            if docname not in self.env.all_docs:
                yield docname
                continue
            targetname = self.get_outfilename(docname)
            try:
                targetmtime = path.getmtime(targetname)
            except Exception:
                targetmtime = 0
            try:
                srcmtime = max(path.getmtime(self.env.doc2path(docname)),
                               template_mtime)
                if srcmtime > targetmtime:
                    yield docname
            except EnvironmentError:
                # source doesn't exist anymore
                pass

    def render_partial(self, node):
        """Utility: Render a lone doctree node."""
        if node is None:
            return {'fragment': ''}
        doc = new_document(b'<partial node>')
        doc.append(node)

        if self._publisher is None:
            self._publisher = Publisher(
                source_class=DocTreeInput,
                destination_class=StringOutput)
            self._publisher.set_components('standalone',
                                           'restructuredtext', 'pseudoxml')

        pub = self._publisher

        pub.reader = DoctreeReader()
        pub.writer = MarkdownWriter(self)
        pub.process_programmatic_settings(
            None, {}, None)
        #pub.process_programmatic_settings(
        #    None, {'output_encoding': 'unicode'}, None)
        pub.set_source(doc, None)
        pub.set_destination(None, None)
        pub.publish()
        return pub.writer.parts

    def prepare_writing(self, docnames):
        # create the search indexer
        self.indexer = None

        self.docwriter = MarkdownWriter(self)
        self.docsettings = OptionParser(
            defaults=self.env.settings,
            components=(self.docwriter,),
            read_config_files=True).get_default_values()
        self.docsettings.compact_lists = bool(self.config.html_compact_lists)

        # determine the additional indices to include
        self.domain_indices = []
        # html_domain_indices can be False/True or a list of index names
        indices_config = self.config.html_domain_indices
        if indices_config:
            for domain_name in sorted(self.env.domains):
                domain = self.env.domains[domain_name]
                for indexcls in domain.indices:
                    indexname = '%s-%s' % (domain.name, indexcls.name)
                    if isinstance(indices_config, list):
                        if indexname not in indices_config:
                            continue
                    # deprecated config value
                    if indexname == 'py-modindex' and \
                       not self.config.html_use_modindex:
                        continue
                    content, collapse = indexcls(domain).generate()
                    if content:
                        self.domain_indices.append(
                            (indexname, indexcls, content, collapse))

        # format the "last updated on" string, only once is enough since it
        # typically doesn't include the time of day
        lufmt = self.config.html_last_updated_fmt
        if lufmt is not None:
            self.last_updated = format_date(lufmt or _('%b %d, %Y'),
                                            language=self.config.language,
                                            warn=self.warn)
        else:
            self.last_updated = None

        self.relations = self.env.collect_relations()

        rellinks = []
        if self.get_builder_config('use_index', 'html'):
            rellinks.append(('genindex', _('General Index'), 'I', _('index')))
        for indexname, indexcls, content, collapse in self.domain_indices:
            # if it has a short name
            if indexcls.shortname:
                rellinks.append((indexname, indexcls.localname,
                                 '', indexcls.shortname))

        self.globalcontext = dict(
            embedded=self.embedded,
            project=self.config.project,
            release=self.config.release,
            version=self.config.version,
            last_updated=self.last_updated,
            copyright=self.config.copyright,
            master_doc=self.config.master_doc,
            docstitle=self.config.html_title,
            shorttitle=self.config.html_short_title,
            show_copyright=self.config.html_show_copyright,
            show_sphinx=self.config.html_show_sphinx,
            has_source=self.config.html_copy_source,
            show_source=self.config.html_show_sourcelink,
            file_suffix=self.out_suffix,
            language=self.config.language,
            sphinx_version=__display_version__,
            rellinks=rellinks,
            builder=self.name,
            parents=[],
        )

    def get_doc_context(self, docname, body):
        """Collect items for the template context of a page."""
        # find out relations
        prev = next = None
        parents = []
        rellinks = self.globalcontext['rellinks'][:]
        related = self.relations.get(docname)
        titles = self.env.titles
        if related and related[2]:
            try:
                next = {
                    'link': self.get_relative_uri(docname, related[2]),
                    'title': self.render_partial(titles[related[2]])['title']
                }
                rellinks.append((related[2], next['title'], 'N', _('next')))
            except KeyError:
                next = None
        if related and related[1]:
            try:
                prev = {
                    'link': self.get_relative_uri(docname, related[1]),
                    'title': self.render_partial(titles[related[1]])['title']
                }
                rellinks.append((related[1], prev['title'], 'P', _('previous')))
            except KeyError:
                # the relation is (somehow) not in the TOC tree, handle
                # that gracefully
                prev = None
        while related and related[0]:
            try:
                parents.append(
                    {'link': self.get_relative_uri(docname, related[0]),
                     'title': self.render_partial(titles[related[0]])['title']})
            except KeyError:
                pass
            related = self.relations.get(related[0])
        if parents:
            # remove link to the master file; we have a generic
            # "back to index" link already
            parents.pop()
        parents.reverse()

        # title rendered as HTML
        title = self.env.longtitles.get(docname)
        title = title and self.render_partial(title)['title'] or ''
        # the name for the copied source
        sourcename = self.config.html_copy_source and docname + '.txt' or ''

        # metadata for the document
        meta = self.env.metadata.get(docname)

        # Suffix for the document
        source_suffix = '.' + self.env.doc2path(docname).split('.')[-1]

        # local TOC and global TOC tree
        self_toc = self.env.get_toc_for(docname, self)
        toc = self.render_partial(self_toc)['fragment']

        return dict(
            parents=parents,
            prev=prev,
            next=next,
            title=title,
            meta=meta,
            body=body,
            rellinks=rellinks,
            sourcename=sourcename,
            toc=toc,
            # only display a TOC if there's more than one item to show
            display_toc=(self.env.toc_num_entries[docname] > 1),
            page_source_suffix=source_suffix,
        )

    def write_doc(self, docname, doctree):
        destination = StringOutput(encoding='utf-8')
        doctree.settings = self.docsettings

        self.secnumbers = self.env.toc_secnumbers.get(docname, {})
        self.fignumbers = self.env.toc_fignumbers.get(docname, {})
        self.imgpath = relative_uri(self.get_target_uri(docname), '_images')
        self.dlpath = relative_uri(self.get_target_uri(docname), '_downloads')
        self.current_docname = docname
        self.docwriter.write(doctree, destination)
        self.docwriter.assemble_parts()
        body = self.docwriter.parts['fragment']

        ctx = self.get_doc_context(docname, body)
        self.handle_page(docname, ctx, event_arg=doctree)

    def write_doc_serialized(self, docname, doctree):
        self.imgpath = relative_uri(self.get_target_uri(docname), self.imagedir)
        self.post_process_images(doctree)
        title = self.env.longtitles.get(docname)
        title = title and self.render_partial(title)['title'] or ''
        self.index_page(docname, doctree, title)

    def finish(self):
        self.finish_tasks.add_task(self.gen_indices)
        self.finish_tasks.add_task(self.gen_additional_pages)
        self.finish_tasks.add_task(self.copy_image_files)
        self.finish_tasks.add_task(self.copy_download_files)
        self.finish_tasks.add_task(self.copy_static_files)
        self.finish_tasks.add_task(self.copy_extra_files)
        self.finish_tasks.add_task(self.write_buildinfo)

        # dump the search index
        self.handle_finish()

    def gen_indices(self):
        self.info(bold('generating indices...'), nonl=1)

        # the global general index
        #if self.get_builder_config('use_index', 'html'):
        #    self.write_genindex()

        # the global domain-specific indices
        self.write_domain_indices()

        self.info()

    def gen_additional_pages(self):
        # pages from extensions
        for pagelist in self.app.emit('html-collect-pages'):
            for pagename, context, template in pagelist:
                self.handle_page(pagename, context, template)

        self.info(bold('writing additional pages...'), nonl=1)

        # additional pages from conf.py
        for pagename, template in self.config.html_additional_pages.items():
            self.info(' '+pagename, nonl=1)
            self.handle_page(pagename, {}, template)

        # the search page
        if self.search:
            self.info(' search', nonl=1)
            self.handle_page('search', {}, 'search.html')

        # the opensearch xml file
        if self.config.html_use_opensearch and self.search:
            self.info(' opensearch', nonl=1)
            fn = path.join(self.outdir, '_static', 'opensearch.xml')
            self.handle_page('opensearch', {}, 'opensearch.xml', outfilename=fn)

        self.info()

    #def write_genindex(self):
    #    # the total count of lines for each index letter, used to distribute
    #    # the entries into two columns
    #    genindex = self.env.create_index(self)
    #    indexcounts = []
    #    for _k, entries in genindex:
    #        indexcounts.append(sum(1 + len(subitems)
    #                               for _, (_, subitems, _) in entries))

    #    genindexcontext = dict(
    #        genindexentries = genindex,
    #        genindexcounts = indexcounts,
    #        split_index = self.config.html_split_index,
    #    )
    #    self.info(' genindex', nonl=1)

    #    if self.config.html_split_index:
    #        self.handle_page('genindex', genindexcontext,
    #                         'genindex-split.html')
    #        self.handle_page('genindex-all', genindexcontext,
    #                         'genindex.html')
    #        for (key, entries), count in zip(genindex, indexcounts):
    #            ctx = {'key': key, 'entries': entries, 'count': count,
    #                   'genindexentries': genindex}
    #            self.handle_page('genindex-' + key, ctx,
    #                             'genindex-single.html')
    #    else:
    #        self.handle_page('genindex', genindexcontext, 'genindex.html')

    def write_domain_indices(self):
        for indexname, indexcls, content, collapse in self.domain_indices:
            indexcontext = dict(
                indextitle=indexcls.localname,
                content=content,
                collapse_index=collapse,
            )
            self.info(' ' + indexname, nonl=1)
            self.handle_page(indexname, indexcontext, 'domainindex.html')

    def copy_image_files(self):
        # copy image files
        if self.images:
            ensuredir(path.join(self.outdir, self.imagedir))
            for src in self.app.status_iterator(self.images, 'copying images... ',
                                                brown, len(self.images)):
                dest = self.images[src]
                try:
                    copyfile(path.join(self.srcdir, src),
                             path.join(self.outdir, self.imagedir, dest))
                except Exception as err:
                    self.warn('cannot copy image file %r: %s' %
                              (path.join(self.srcdir, src), err))

    def copy_download_files(self):
        def to_relpath(f):
            return relative_path(self.srcdir, f)
        # copy downloadable files
        if self.env.dlfiles:
            ensuredir(path.join(self.outdir, '_downloads'))
            for src in self.app.status_iterator(self.env.dlfiles,
                                                'copying downloadable files... ',
                                                brown, len(self.env.dlfiles),
                                                stringify_func=to_relpath):
                dest = self.env.dlfiles[src][1]
                try:
                    copyfile(path.join(self.srcdir, src),
                             path.join(self.outdir, '_downloads', dest))
                except Exception as err:
                    self.warn('cannot copy downloadable file %r: %s' %
                              (path.join(self.srcdir, src), err))

    def copy_static_files(self):
        # copy static files
        self.info(bold('copying static files... '), nonl=True)
        ensuredir(path.join(self.outdir, '_static'))

        ctx = self.globalcontext.copy()

        # copy over all user-supplied static files
        staticentries = [path.join(self.confdir, spath)
                         for spath in self.config.html_static_path]
        matchers = compile_matchers(self.config.exclude_patterns)
        for entry in staticentries:
            if not path.exists(entry):
                self.warn('html_static_path entry %r does not exist' % entry)
                continue
            copy_static_entry(entry, path.join(self.outdir, '_static'), self,
                              ctx, exclude_matchers=matchers)
        self.info('done')

    def copy_extra_files(self):
        # copy html_extra_path files
        self.info(bold('copying extra files... '), nonl=True)
        extraentries = [path.join(self.confdir, epath)
                        for epath in self.config.html_extra_path]
        matchers = compile_matchers(self.config.exclude_patterns)
        for entry in extraentries:
            if not path.exists(entry):
                self.warn('html_extra_path entry %r does not exist' % entry)
                continue
            copy_extra_entry(entry, self.outdir, matchers)
        self.info('done')

    def write_buildinfo(self):
        # write build info file
        fp = open(path.join(self.outdir, '.buildinfo'), 'w')
        try:
            fp.write('# Sphinx build info version 1\n'
                     '# This file hashes the configuration used when building'
                     ' these files. When it is not found, a full rebuild will'
                     ' be done.\nconfig: %s\ntags: %s\n' %
                     (self.config_hash, self.tags_hash))
        finally:
            fp.close()

    def cleanup(self):
        pass

    def post_process_images(self, doctree):
        """Pick the best candidate for an image and link down-scaled images to
        their high res version.
        """
        Builder.post_process_images(self, doctree)

        if self.config.html_scaled_image_link:
            for node in doctree.traverse(nodes.image):
                scale_keys = ('scale', 'width', 'height')
                if not any((key in node) for key in scale_keys) or \
                   isinstance(node.parent, nodes.reference):
                    # docutils does unfortunately not preserve the
                    # ``target`` attribute on images, so we need to check
                    # the parent node here.
                    continue
                uri = node['uri']
                reference = nodes.reference('', '', internal=True)
                if uri in self.images:
                    reference['refuri'] = posixpath.join(self.imgpath,
                                                         self.images[uri])
                else:
                    reference['refuri'] = uri
                node.replace_self(reference)
                reference.append(node)

    def load_indexer(self, docnames):
        keep = set(self.env.all_docs) - set(docnames)
        try:
            searchindexfn = path.join(self.outdir, self.searchindex_filename)
            if self.indexer_dumps_unicode:
                f = codecs.open(searchindexfn, 'r', encoding='utf-8')
            else:
                f = open(searchindexfn, 'rb')
            try:
                self.indexer.load(f, self.indexer_format)
            finally:
                f.close()
        except (IOError, OSError, ValueError):
            if keep:
                self.warn('search index couldn\'t be loaded, but not all '
                          'documents will be built: the index will be '
                          'incomplete.')
        # delete all entries for files that will be rebuilt
        self.indexer.prune(keep)

    def index_page(self, pagename, doctree, title):
        # only index pages with title
        if self.indexer is not None and title:
            self.indexer.feed(pagename, title, doctree)

    def _get_local_toctree(self, docname, collapse=True, **kwds):
        if 'includehidden' not in kwds:
            kwds['includehidden'] = False
        return self.render_partial(self.env.get_toctree_for(
            docname, self, collapse, **kwds))['fragment']

    # --------- these are overwritten by the serialization builder

    def handle_page(self, pagename, addctx, templatename='page.html',
                    outfilename=None, event_arg=None):
        ctx = self.globalcontext.copy()
        # current_page_name is backwards compatibility
        ctx['pagename'] = ctx['current_page_name'] = pagename
        default_baseuri = self.get_target_uri(pagename)
        # in the singlehtml builder, default_baseuri still contains an #anchor
        # part, which relative_uri doesn't really like...
        default_baseuri = default_baseuri.rsplit('#', 1)[0]

        def pathto(otheruri, resource=False, baseuri=default_baseuri):
            if resource and '://' in otheruri:
                # allow non-local resources given by scheme
                return otheruri
            elif not resource:
                otheruri = self.get_target_uri(otheruri)
            uri = relative_uri(baseuri, otheruri) or '#'
            return uri
        ctx['pathto'] = pathto
        ctx['hasdoc'] = lambda name: name in self.env.all_docs
        if self.name != 'mdhelp':
            ctx['encoding'] = encoding = self.config.html_output_encoding
        else:
            ctx['encoding'] = encoding = self.encoding
        ctx['toctree'] = lambda **kw: self._get_local_toctree(pagename, **kw)
        ctx.update(addctx)

        newtmpl = self.app.emit_firstresult('md-page-context', pagename,
                                            templatename, ctx, event_arg)
        if newtmpl:
            templatename = newtmpl

        try:
            output = self.templates.render(templatename, ctx)
        except UnicodeError:
            self.warn("a Unicode error occurred when rendering the page %s. "
                      "Please make sure all config values that contain "
                      "non-ASCII content are Unicode strings." % pagename)
            return

        if not outfilename:
            outfilename = self.get_outfilename(pagename)
        # outfilename's path is in general different from self.outdir
        ensuredir(path.dirname(outfilename))
        try:
            f = codecs.open(outfilename, 'w', encoding, 'xmlcharrefreplace')
            try:
                f.write(output)
            finally:
                f.close()
        except (IOError, OSError) as err:
            self.warn("error writing file %s: %s" % (outfilename, err))
        if self.copysource and ctx.get('sourcename'):
            # copy the source file for the "show source" link
            source_name = path.join(self.outdir, '_sources',
                                    os_path(ctx['sourcename']))
            ensuredir(path.dirname(source_name))
            copyfile(self.env.doc2path(pagename), source_name)

    def handle_finish(self):
        if self.indexer:
            self.finish_tasks.add_task(self.dump_search_index)
        self.finish_tasks.add_task(self.dump_inventory)

    def dump_inventory(self):
        self.info(bold('dumping object inventory... '), nonl=True)
        f = open(path.join(self.outdir, INVENTORY_FILENAME), 'wb')
        try:
            f.write((u'# Sphinx inventory version 2\n'
                     u'# Project: %s\n'
                     u'# Version: %s\n'
                     u'# The remainder of this file is compressed using zlib.\n'
                     % (self.config.project, self.config.version)).encode('utf-8'))
            compressor = zlib.compressobj(9)
            for domainname, domain in sorted(self.env.domains.items()):
                for name, dispname, type, docname, anchor, prio in \
                        sorted(domain.get_objects()):
                    if anchor.endswith(name):
                        # this can shorten the inventory by as much as 25%
                        anchor = anchor[:-len(name)] + '$'
                    uri = self.get_target_uri(docname)
                    if anchor:
                        uri += '#' + anchor
                    if dispname == name:
                        dispname = u'-'
                    f.write(compressor.compress(
                        (u'%s %s:%s %s %s %s\n' % (name, domainname, type,
                                                   prio, uri, dispname)).encode('utf-8')))
            f.write(compressor.flush())
        finally:
            f.close()
        self.info('done')

    def dump_search_index(self):
        self.info(
            bold('dumping search index in %s ... ' % self.indexer.label()),
            nonl=True)
        self.indexer.prune(self.env.all_docs)
        searchindexfn = path.join(self.outdir, self.searchindex_filename)
        # first write to a temporary file, so that if dumping fails,
        # the existing index won't be overwritten
        if self.indexer_dumps_unicode:
            f = codecs.open(searchindexfn + '.tmp', 'w', encoding='utf-8')
        else:
            f = open(searchindexfn + '.tmp', 'wb')
        try:
            self.indexer.dump(f, self.indexer_format)
        finally:
            f.close()
        movefile(searchindexfn + '.tmp', searchindexfn)
        self.info('done')


def setup(app):
    #app.add_config_value('todo_include_todos', False, 'html')

    #app.add_node(todolist)
    #app.add_node(todo,
    #             html=(visit_todo_node, depart_todo_node),
    #             latex=(visit_todo_node, depart_todo_node),
    #             text=(visit_todo_node, depart_todo_node))

    #app.add_directive('todo', TodoDirective)
    #app.add_directive('todolist', TodolistDirective)
    #app.connect('doctree-resolved', process_todo_nodes)
    #app.connect('env-purge-doc', purge_todos)

    app.add_builder(MarkdownBuilder)

    return {'version': '0.1'}   # identifies the version of our extension
