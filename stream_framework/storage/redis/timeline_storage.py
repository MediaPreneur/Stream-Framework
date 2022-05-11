from stream_framework.storage.base import BaseTimelineStorage
from stream_framework.storage.redis.structures.sorted_set import RedisSortedSetCache
from stream_framework.storage.redis.connection import get_redis_connection


class TimelineCache(RedisSortedSetCache):
    sort_asc = False


class RedisTimelineStorage(BaseTimelineStorage):

    def get_cache(self, key):
        return TimelineCache(key)

    def contains(self, key, activity_id):
        cache = self.get_cache(key)
        return cache.contains(activity_id)

    def get_slice_from_storage(self, key, start, stop, filter_kwargs=None, ordering_args=None):
        '''
        Returns a slice from the storage
        :param key: the redis key at which the sorted set is located
        :param start: the start
        :param stop: the stop
        :param filter_kwargs: a dict of filter kwargs
        :param ordering_args: a list of fields used for sorting

        **Example**::
           get_slice_from_storage('feed:13', 0, 10, {activity_id__lte=10})
        '''
        cache = self.get_cache(key)

        # parse the filter kwargs and translate them to min max
        # as used by the get results function
        valid_kwargs = [
            'activity_id__gte', 'activity_id__lte',
            'activity_id__gt', 'activity_id__lt',
        ]
        filter_kwargs = filter_kwargs or {}
        result_kwargs = {}
        for k in valid_kwargs:
            v = filter_kwargs.pop(k, None)
            if v is not None:
                if not isinstance(v, (float, int, long)):
                    raise ValueError(
                        f'Filter kwarg values should be floats, int or long, got {k}={v}'
                    )


                # By default, the interval specified by min_score and max_score is closed (inclusive).
                # It is possible to specify an open interval (exclusive) by prefixing the score with the character (
                _, direction = k.split('__')
                equal = 'te' in direction

                if not equal:
                    v = f'({str(v)}'
                if 'gt' in direction:
                    result_kwargs['min_score'] = v
                else:
                    result_kwargs['max_score'] = v
        # complain if we didn't recognize the filter kwargs
        if filter_kwargs:
            raise ValueError(f'Unrecognized filter kwargs {filter_kwargs}')

        if ordering_args:
            if len(ordering_args) > 1:
                raise ValueError(f'Too many order kwargs {ordering_args}')

            if '-activity_id' in ordering_args:
                # descending sort
                cache.sort_asc = False
            elif 'activity_id' in ordering_args:
                cache.sort_asc = True
            else:
                raise ValueError(f'Unrecognized order kwargs {ordering_args}')

        # get the actual results
        key_score_pairs = cache.get_results(start, stop, **result_kwargs)
        return [(score, data) for data, score in key_score_pairs]

    def get_batch_interface(self):
        return get_redis_connection().pipeline(transaction=False)

    def get_index_of(self, key, activity_id):
        cache = self.get_cache(key)
        return cache.index_of(activity_id)

    def add_to_storage(self, key, activities, batch_interface=None):
        cache = self.get_cache(key)
        # turn it into key value pairs
        scores = map(long, activities.keys())
        score_value_pairs = zip(scores, activities.values())
        result = cache.add_many(score_value_pairs)
        for r in result:
            # errors in strings?
            # anyhow raise them here :)
            if hasattr(r, 'isdigit') and not r.isdigit():
                raise ValueError(f'got error {r} in results {result}')
        return result

    def remove_from_storage(self, key, activities, batch_interface=None):
        cache = self.get_cache(key)
        return cache.remove_many(activities.values())

    def count(self, key):
        cache = self.get_cache(key)
        return int(cache.count())

    def delete(self, key):
        cache = self.get_cache(key)
        cache.delete()

    def trim(self, key, length, batch_interface=None):
        cache = self.get_cache(key)
        cache.trim(length)
