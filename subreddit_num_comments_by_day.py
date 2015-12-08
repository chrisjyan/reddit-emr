from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, JSONValueProtocol
from mrjob.step import MRStep
import datetime

class MRSubredditNumCommentsByDay(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_subreddit_day,
                combiner=self.combiner_subreddit_day,
                reducer=self.reducer_subreddit_day)
        ]

    def mapper_subreddit_day(self, _, comment):
        subreddit = comment['subreddit']
        created_utc = comment['created_utc']
        created_date = str(datetime.date.fromtimestamp(int(created_utc)))
        yield {'subreddit': subreddit, 'created_date': created_date}, 1

    def combiner_subreddit_day(self, subreddit_created_date, comment_freqs):
        yield subreddit_created_date, sum(comment_freqs)

    def reducer_subreddit_day(self, subreddit_created_date, comment_freqs):
        yield subreddit_created_date, sum(comment_freqs)

if __name__ == '__main__':
    MRSubredditNumCommentsByDay.run()
