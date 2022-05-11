import pickle
from feedly.serializers.base import BaseSerializer, BaseAggregatedSerializer


class PickleSerializer(BaseSerializer):

    def loads(self, serialized_activity):
        return pickle.loads(serialized_activity)

    def dumps(self, activity):
        self.check_type(activity)
        return pickle.dumps(activity)


class AggregatedActivityPickleSerializer(BaseAggregatedSerializer):
    #: indicates if dumps returns dehydrated aggregated activities
    dehydrate = True

    def loads(self, serialized_data):
        return pickle.loads(serialized_data)

    def dumps(self, aggregated):
        self.check_type(aggregated)
        if not aggregated.dehydrated:
            aggregated = aggregated.get_dehydrated()
        return pickle.dumps(aggregated)
