from stream_framework.serializers.aggregated_activity_serializer import AggregatedActivitySerializer
import pickle


class CassandraAggregatedActivitySerializer(AggregatedActivitySerializer):

    def __init__(self, model, *args, **kwargs):
        AggregatedActivitySerializer.__init__(self, *args, **kwargs)
        self.model = model

    def dumps(self, aggregated):
        activities = pickle.dumps(aggregated.activities)
        return self.model(
            activity_id=long(aggregated.serialization_id),
            activities=activities,
            group=aggregated.group,
            created_at=aggregated.created_at,
            updated_at=aggregated.updated_at,
        )

    def loads(self, serialized_aggregated):
        activities = pickle.loads(serialized_aggregated.activities)
        return self.aggregated_activity_class(
            group=serialized_aggregated.group,
            activities=activities,
            created_at=serialized_aggregated.created_at,
            updated_at=serialized_aggregated.updated_at,
        )
