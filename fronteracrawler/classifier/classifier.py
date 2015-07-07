from __future__ import division
from nltk import word_tokenize
import sys


class TopicClassifier(object):

    def __init__(self):
        self.topic = {}
        self.threshold = 0.0
        self.exclude_set = set()

    @classmethod
    def from_file(cls, prob_dict):
        obj = cls()
        fh = open(prob_dict)
        sum = 0
        for line in fh:
            (word, sep, freq) = line.strip().partition('\t')
            freq_int = int(freq)
            obj.topic[word] = freq_int
            sum += freq_int

        for (k, v) in obj.topic.iteritems(): obj.topic[k] = v / float(sum)

    @classmethod
    def from_keywords(cls, include, exclude):
        obj = cls()
        count = len(include) + len(exclude)
        p = 1.0 / count
        for word in include: obj.topic[word] = p
        obj.exclude_set = set(exclude)
        obj.threshold = p
        return obj

    def score_paragraphs(self, paragraphs):
        topic_probability = 0.0
        for p in paragraphs:
            for token in word_tokenize(p):
                if token in self.topic:
                    topic_probability += self.topic[token]
                    continue
                if token in self.exclude_set:
                    return None
        return topic_probability

    def classify_paragraphs(self, score):
        if not score:
            return False
        return True if score > self.threshold else False


if __name__ == '__main__':
    tc = TopicClassifier.from_file(sys.argv[1])
    print tc.topic['escorts']