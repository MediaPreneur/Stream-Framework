from __future__ import absolute_import
from feedly.metrics.base import Metrics
from statsd import StatsClient


class StatsdMetrics(Metrics):

    def __init__(self, host='localhost', port=8125, prefix=None):
        self.statsd = StatsClient(host, port, prefix)

    def fanout_timer(self, feed_class):
        return self.statsd.timer(f'{feed_class.__name__}.fanout_latency')

    def feed_reads_timer(self, feed_class):
        return self.statsd.timer(f'{feed_class.__name__}.read_latency')

    def on_feed_read(self, feed_class, activities_count):
        self.statsd.incr(f'{feed_class.__name__}.reads', activities_count)

    def on_feed_write(self, feed_class, activities_count):
        self.statsd.incr(f'{feed_class.__name__}.writes', activities_count)

    def on_feed_remove(self, feed_class, activities_count):
        self.statsd.incr(f'{feed_class.__name__}.deletes', activities_count)

    def on_fanout(self, feed_class, operation, activities_count=1):
        metric = (feed_class.__name__, operation.__name__)
        self.statsd.incr('%s.fanout.%s' % metric, activities_count)

    def on_activity_published(self):
        self.statsd.incr('activities.published')

    def on_activity_removed(self):
        self.statsd.incr('activities.removed')
