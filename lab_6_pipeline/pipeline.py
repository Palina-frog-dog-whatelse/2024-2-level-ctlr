"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import json
import pathlib
import re
from typing import cast

import spacy_udpipe
from networkx import DiGraph
from spacy_conll import ConllParser  # type: ignore[import-not-found, import-untyped]

from core_utils.article import io
from core_utils.article.article import Article, ArtifactType
from core_utils.constants import ASSETS_PATH, PROJECT_ROOT
from core_utils.pipeline import (
    AbstractCoNLLUAnalyzer,
    CoNLLUDocument,
    LibraryWrapper,
    PipelineProtocol,
    StanzaDocument,
    TreeNode,
    UDPipeDocument,
    UnifiedCoNLLUDocument,
)
from core_utils.visualizer import visualize


class InconsistentDatasetError(Exception):
    """
    Raised when file IDs contain slips,
    number of meta and raw files is not equal,
    or files are empty.
    """


class EmptyDirectoryError(Exception):
    """
    Raised when the directory is empty.
    """


class EmptyFileError(Exception):
    """
    Raised when the file is empty.
    """

class CorpusManager:
    """
    Work with articles and store them.
    """

    def __init__(self, path_to_raw_txt_data: pathlib.Path) -> None:
        """
        Initialize an instance of the CorpusManager class.

        Args:
            path_to_raw_txt_data (pathlib.Path): Path to raw txt data
        """
        self.path_to_raw = path_to_raw_txt_data
        self._storage = {}
        self._validate_dataset()
        self._scan_dataset()

    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self.path_to_raw.exists():
            raise FileNotFoundError("The path to articles doesn't lead to an existing directory")
        if not self.path_to_raw.is_dir():
            raise NotADirectoryError("The path to articles leads to a file, not a directory")
        if not any(self.path_to_raw.iterdir()):
            raise EmptyDirectoryError("The path to articles leads to an empty directory")
        ind = 1
        files_to_check_raw = [int(file.name[:file.name.index("_")])
                              for file in self.path_to_raw.iterdir()
                          if file.is_file() and file.name.endswith("_raw.txt")]
        files_to_check_meta = [int(file.name[:file.name.index("_")])
                               for file in self.path_to_raw.iterdir()
                              if file.is_file() and file.name.endswith("_meta.json")]
        for file_ind in sorted(files_to_check_raw):
            if file_ind == ind and ind in files_to_check_meta:
                ind += 1
            else:
                raise InconsistentDatasetError(
                    "There are slips in file IDs or the number of raw and meta files is not equal"
                )
            if not ((self.path_to_raw / (str(file_ind)+"_raw.txt")).stat().st_size
                    or not (self.path_to_raw / (str(file_ind)+"_meta.json")).stat().st_size):
                raise InconsistentDatasetError("At least one file is empty")

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        storage_files = tuple(self.path_to_raw.iterdir())
        file_names = {file.name: file for file in storage_files}
        for file in storage_files:
            if re.match("[0-9]+_raw.txt", file.name):
                article_id = int(file.stem[:file.stem.index("_")])
                article_url: str | None = None
                article_meta = file_names.get(str(article_id) + "_meta.json", "")
                if article_meta:
                    with open(article_meta, "r", encoding="utf-8") as meta_file:
                        article_url = json.load(meta_file)["url"]
                article = io.from_raw(file, Article(url=article_url, article_id=article_id))
                # I know that pipeline doesn't need to know article URLs,
                # but the scraper tests work on students' data and fail if URLs are empty
                self._storage[article_id] = article

    def get_articles(self) -> dict:
        """
        Get storage params.

        Returns:
            dict: Storage params
        """
        return self._storage


class TextProcessingPipeline(PipelineProtocol):
    """
    Preprocess and morphologically annotate sentences into the CONLL-U format.
    """

    def __init__(
        self, corpus_manager: CorpusManager, analyzer: LibraryWrapper | None = None
    ) -> None:
        """
        Initialize an instance of the TextProcessingPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper | None): Analyzer instance
        """
        self._corpus = corpus_manager
        self.analyzer = analyzer

    def run(self) -> None:
        """
        Perform basic preprocessing and write processed text to files.
        """
        articles = self._corpus.get_articles()
        listed_articles = list(articles.values())
        analyzed_articles = []
        if self.analyzer:
            # Clean first
            for article in listed_articles:
                io.from_raw(article)  # Load raw if needed
                cleaned_text = article.text.lower().translate(str.maketrans('', '', string.punctuation))
                article.text = cleaned_text
                io.to_cleaned(article)

            # Analyze cleaned
            analyzed_articles = self.analyzer.analyze([art.text for art in listed_articles])

            for art_id, article in enumerate(listed_articles):
                article.set_conllu_info(analyzed_articles[art_id])
                self.analyzer.to_conllu(article)
        else:
            for article in listed_articles:
                io.from_raw(article)
                cleaned_text = article.text.lower().translate(str.maketrans('', '', string.punctuation))
                article.text = cleaned_text
                io.to_cleaned(article)


class UDPipeAnalyzer(LibraryWrapper):
    """
    Wrapper for udpipe library.
    """

    #: Analyzer
    _analyzer: AbstractCoNLLUAnalyzer

    def __init__(self) -> None:
        """
        Initialize an instance of the UDPipeAnalyzer class.
        """
        self._analyzer = self._bootstrap()

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Load and set up the UDPipe model.

        Returns:
            AbstractCoNLLUAnalyzer: Analyzer instance
        """
        ru = spacy_udpipe.load_from_path(
            "ru",
            str(PROJECT_ROOT / "lab_6_pipeline" / "assets" / "model" /
                "russian-syntagrus-ud-2.0-170801.udpipe")
        )
        ru.add_pipe(
            "conll_formatter",
            last=True,
            config={"conversion_maps": {"XPOS": {"": "_"}}, "include_headers": True},
        )
        return ru

    def analyze(self, texts: list[str]) -> list[UDPipeDocument | str]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[UDPipeDocument | str]: List of documents
        """
        analyzed_texts = []
        for text in texts:
            analyzed_text = self._analyzer(text)
            annotation = analyzed_text._.conll_str
            analyzed_texts.append(annotation)
        return analyzed_texts

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        conllu_info = article.get_conllu_info()
        conllu_path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        with open(conllu_path, "w", encoding="utf-8") as conllu_f:
            conllu_f.write(conllu_info)
            conllu_f.write("\n")

    def from_conllu(self, article: Article) -> UDPipeDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            UDPipeDocument: Document ready for parsing
        """
        conllu_path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        if not conllu_path.stat().st_size:
            raise EmptyFileError("At least one conllu file is empty")
        with open(conllu_path, "r", encoding="utf-8") as conllu_file:
            conllu_content = conllu_file.read()[:-1]
        parser = ConllParser(self._analyzer)
        doc = parser.parse_conll_text_as_spacy(conllu_content)
        if not doc._.conll_str:
            raise EmptyFileError("The conllu file is empty")
        return cast(UDPipeDocument, doc)

    def get_document(self, doc: UDPipeDocument) -> UnifiedCoNLLUDocument:
        """
        Present ConLLU document's sentence tokens as a unified structure.

        Args:
            doc (UDPipeDocument): ConLLU document

        Returns:
            UnifiedCoNLLUDocument: Dictionary of token features within document sentences
        """

        def get_document(self, doc: UDPipeDocument) -> UnifiedCoNLLUDocument:
            sentences = []
            for sent in doc.sents:
                words = []
                for token in sent:
                    word = ConLLUWord(
                        id=token.i + 1,  # 1-based ID
                        form=token.text,
                        lemma=token.lemma_ or '_',
                        upos=token.pos_ or '_',
                        xpos=token.tag_ or '_',
                        feats=str(token.morph) or '_',
                        head=token.head.i if hasattr(token.head, 'i') else 0,
                        deprel=token.dep_ or '_',
                        deps='_',
                        misc='_'
                    )
                    words.append(word)
                sentences.append(ConLLUSentence(words=words))
            return UnifiedCoNLLUDocument(sentences=sentences)


class StanzaAnalyzer(LibraryWrapper):
    """
    Wrapper for stanza library.
    """

    #: Analyzer
    _analyzer: AbstractCoNLLUAnalyzer

    def __init__(self) -> None:
        """
        Initialize an instance of the StanzaAnalyzer class.
        """

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Load and set up the Stanza model.

        Returns:
            AbstractCoNLLUAnalyzer: Analyzer instance
        """

    def analyze(self, texts: list[str]) -> list[StanzaDocument]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[StanzaDocument]: List of documents
        """

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """

    def from_conllu(self, article: Article) -> StanzaDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            StanzaDocument: Document ready for parsing
        """

    def get_document(self, doc: StanzaDocument) -> UnifiedCoNLLUDocument:
        """
        Present ConLLU document's sentence tokens as a unified structure.

        Args:
            doc (StanzaDocument): ConLLU document

        Returns:
            UnifiedCoNLLUDocument: Document of token features within document sentences
        """


class POSFrequencyPipeline:
    """
    Count frequencies of each POS in articles, update meta info and produce graphic report.
    """

    def __init__(self, corpus_manager: CorpusManager, analyzer: LibraryWrapper) -> None:
        """
        Initialize an instance of the POSFrequencyPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper): Analyzer instance
        """
        self._corpus = corpus_manager
        self.analyzer = analyzer

    def _count_frequencies(self, unified_doc: UnifiedCoNLLUDocument) -> dict[str, int]:
        """
        Count POS frequency in Article.

        Args:
            article (Article): Article instance

        Returns:
            dict[str, int]: POS frequencies
        """
        from collections import Counter
        pos_counter = Counter()
        for sentence in unified_doc.sentences:
            for word in sentence.words:
                if word.upos != '_':
                    pos_counter[word.upos] += 1
        return dict(pos_counter)

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        for article in self._corpus.get_articles().values():
            ud_data = self.analyzer.from_conllu(article)
            unified_doc = self.analyzer.get_document(ud_data)
            freq = self._count_frequencies(unified_doc)
            article.set_pos_info(freq)
            io.to_meta(article)
            visualize(article, ASSETS_PATH / f"{article.article_id}_image.png")

class PatternSearchPipeline(PipelineProtocol):
    """
    Search for the required syntactic pattern.
    """

    def __init__(
        self, corpus_manager: CorpusManager, analyzer: LibraryWrapper, pos: tuple[str, ...]
    ) -> None:
        """
        Initialize an instance of the PatternSearchPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper): Analyzer instance
            pos (tuple[str, ...]): Root, Dependency, Child part of speech
        """

    def _make_graphs(self, doc: CoNLLUDocument) -> list[DiGraph]:
        """
        Make graphs for a document.

        Args:
            doc (CoNLLUDocument): Document for patterns searching

        Returns:
            list[DiGraph]: Graphs for the sentences in the document
        """

    def _add_children(
        self, graph: DiGraph, subgraph_to_graph: dict, node_id: int, tree_node: TreeNode
    ) -> None:
        """
        Add children to TreeNode.

        Args:
            graph (DiGraph): Sentence graph to search for a pattern
            subgraph_to_graph (dict): Matched subgraph
            node_id (int): ID of root node of the match
            tree_node (TreeNode): Root node of the match
        """

    def _find_pattern(self, doc_graphs: list) -> dict[int, list[TreeNode]]:
        """
        Search for the required pattern.

        Args:
            doc_graphs (list): A list of graphs for the document

        Returns:
            dict[int, list[TreeNode]]: A dictionary with pattern matches
        """

    def run(self) -> None:
        """
        Search for a pattern in documents and writes found information to JSON file.
        """


def main() -> None:
    """
    Entrypoint for pipeline module.
    """
    manager = CorpusManager(path_to_raw_txt_data=ASSETS_PATH)
    udpipe_analyzer = UDPipeAnalyzer()
    pipeline = TextProcessingPipeline(manager, udpipe_analyzer)
    pipeline.run()
    pos_pipeline = POSFrequencyPipeline(manager, udpipe_analyzer)
    pos_pipeline.run()


if __name__ == "__main__":
    main()