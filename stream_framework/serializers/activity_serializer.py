from stream_framework.serializers.base import BaseSerializer
from stream_framework.utils import epoch_to_datetime, datetime_to_epoch
from stream_framework.verbs import get_verb_by_id
import pickle


class ActivitySerializer(BaseSerializer):

    '''
    Serializer optimized for taking as little memory as possible to store an
    Activity

    Serialization consists of 5 parts
    - actor_id
    - verb_id
    - object_id
    - target_id
    - extra_context (pickle)

    None values are stored as 0
    '''

    def dumps(self, activity):
        self.check_type(activity)
        # keep the milliseconds
        activity_time = '%.6f' % datetime_to_epoch(activity.time)
        parts = [activity.actor_id, activity.verb.id,
                 activity.object_id, activity.target_id or 0]
        if extra_context := activity.extra_context.copy():
            pickle_string = pickle.dumps(activity.extra_context)
        else:
            pickle_string = ''
        parts += [activity_time, pickle_string]
        return ','.join(map(str, parts))

    def loads(self, serialized_activity):
        parts = serialized_activity.split(',')
        # convert these to ids
        actor_id, verb_id, object_id, target_id = map(
            int, parts[:4])
        activity_datetime = epoch_to_datetime(float(parts[4]))
        pickle_string = str(parts[5])
        if not target_id:
            target_id = None
        verb = get_verb_by_id(verb_id)
        extra_context = pickle.loads(pickle_string) if pickle_string else {}
        return self.activity_class(
            actor_id,
            verb,
            object_id,
            target_id,
            time=activity_datetime,
            extra_context=extra_context,
        )
