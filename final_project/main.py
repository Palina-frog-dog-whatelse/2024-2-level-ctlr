"""
Final project implementation.
"""

# pylint: disable=unused-import
import shutil
from pathlib import Path

from core_utils.constants import PROJECT_ROOT
from lab_6_pipeline.pipeline import UDPipeAnalyzer


class CorpusHandler:
    """
    Class for processing a corpus of texts: merging and analysis using UDPipe.
    """
    def __init__(self, input_dir: Path):
        self.input_dir = input_dir
        self.combined_path = self.input_dir / "corpus.txt"
        self.combined_content = ""
        self.udpipe = UDPipeAnalyzer()

    def combine_texts(self) -> None:
        """
        Merges texts from files starting with 'cern'.
        """
        text_parts = []
        for file_path in self.input_dir.iterdir():
            if file_path.stem.startswith('cern') and file_path.is_file():
                with open(file_path, 'r', encoding='utf-8') as file:
                    text_parts.append(file.read())
        self.combined_content = '\n\n'.join(text_parts)

    def save_combined(self) -> None:
        """
        Saves the merged text to a file.
        """
        with open(self.combined_path, 'w', encoding='utf-8') as file:
            file.write(self.combined_content)

    def analyze_to_conllu(self, output_dir: Path) -> None:
        """
        Analyzes the text using UDPipe and saves to .conllu.
        """
        with open(self.combined_path, 'r', encoding='utf-8') as file:
            texts = [file.read()]
            conllu_output = self.udpipe.analyze(texts=texts)

        conllu_file = output_dir / 'auto_annotated.conllu'
        with open(conllu_file, 'w', encoding='utf-8') as file:
            file.write('\n'.join(map(str, conllu_output)))
            file.write('\n')


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    corpus_path = PROJECT_ROOT / "final_project" / "assets" / "cerny"
    dist_path = PROJECT_ROOT / "final_project" / "dist"

    handler = CorpusHandler(corpus_path)
    handler.combine_texts()
    handler.save_combined()

    if dist_path.exists():
        shutil.rmtree(dist_path)
    dist_path.mkdir(parents=True, exist_ok=True)

    handler.analyze_to_conllu(dist_path)


if __name__ == "__main__":
    main()

