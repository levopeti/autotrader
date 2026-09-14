from .dispatch import parse as dispatch_parse
from .regex_parser import RegexSignalParser
from .regex_parser import parse as generic_parse


# Top-level `parse` a dispatch-et használja (chat_id-vel). Visszafele
# kompatibilis: ha a hívó nem ad chat_id-t, a generic parser fut.
parse = dispatch_parse


__all__ = ["RegexSignalParser", "parse", "generic_parse", "dispatch_parse"]
