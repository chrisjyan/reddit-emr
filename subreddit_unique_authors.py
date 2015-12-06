from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, JSONValueProtocol
from mrjob.step import MRStep

class MRSubredditUniqueAuthors(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def steps(self):
        return [
            MRStep(
                mapper_init=self.init_get_authors,
                mapper=self.mapper_get_authors,
                combiner=self.combiner_unique_authors,
                reducer=self.reducer_unique_authors)
        ]

    def configure_options(self):
        super(MRSubredditUniqueAuthors, self).configure_options()
        self.add_file_option('--subreddit_list')

    def init_get_authors(self):
        """Set subreddits from which to yield authors.

        If self.options.subreddit_list file is not set or contains
        no subreddits, authors are yielded from all subreddits.
        """
        self.subreddit_set = set()
        if self.options.subreddit_list is not None:
            with open(self.options.subreddit_list, 'r') as f:
                for line in f:
                    self.subreddit_set.add(line.strip())
        self.use_all_subreddits = (len(self.subreddit_set) == 0)

    def mapper_get_authors(self, _, comment):
        subreddit = comment['subreddit']
        author = comment['author']
        if self.use_all_subreddits or subreddit in self.subreddit_set:
            yield subreddit, author

    def combiner_unique_authors(self, subreddit, authors):
        unique_authors = list(set(authors))
        for author in unique_authors:
            yield subreddit, author

    def reducer_unique_authors(self, subreddit, authors):
        unique_authors = list(set(authors))
        yield subreddit, unique_authors

if __name__ == '__main__':
    MRSubredditUniqueAuthors.run()
