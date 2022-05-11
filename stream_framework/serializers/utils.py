from stream_framework.exceptions import SerializationException


def check_reserved(value, reserved_characters):
    if any(reserved in value for reserved in reserved_characters):
        raise SerializationException(
            f'encountered reserved character {reserved} in {value}'
        )
