from feedly.storage.base import BaseActivityStorage
from feedly.storage.redis.structures.hash import ShardedHashCache
from feedly.serializers.activity_serializer import ActivitySerializer


class ActivityCache(ShardedHashCache):
    key_format = 'activity:cache:%s'


class RedisActivityStorage(BaseActivityStorage):
    default_serializer_class = ActivitySerializer

    def get_key(self):
        return self.options.get('key', 'global')

    def get_cache(self):
        key = self.get_key()
        return ActivityCache(key)

    def get_from_storage(self, activity_ids, *args, **kwargs):
        cache = self.get_cache()
        activities = cache.get_many(activity_ids)
        activities = {k: unicode(v) for k, v in activities.items() if v}
        return activities

    def add_to_storage(self, serialized_activities, *args, **kwargs):
        cache = self.get_cache()
        key_value_pairs = serialized_activities.items()
        return (
            len(key_value_pairs)
            if (result := cache.set_many(key_value_pairs))
            else 0
        )

    def remove_from_storage(self, activity_ids, *args, **kwargs):
        # we never explicitly remove things from storage
        cache = self.get_cache()
        return cache.delete_many(activity_ids)

    def flush(self):
        cache = self.get_cache()
        cache.delete()
