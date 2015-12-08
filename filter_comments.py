from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, JSONValueProtocol
from mrjob.step import MRStep
import re

SARCASM_S_RE = re.compile(r"((?<=\W)|^)/s((?=\W)|$)")
SARCASM_KAPPA_RE = re.compile(r"kappa")

class MRFilterComments(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_filter_comments,
                reducer=self.reducer_unique_authors)
        ]

    def mapper_filter_comments(self, _, comment):
        body = comment['body']
        if SARCASM_S_RE.search(body) is not None:
            yield 's', comment
        if SARCASM_KAPPA_RE.search(body) is not None:
            yield 'kappa', comment

    def reducer_unique_authors(self, sarcasm_type, comments):
        for comment in comments:
            yield sarcasm_type, comment

if __name__ == '__main__':
    MRFilterComments.run()
