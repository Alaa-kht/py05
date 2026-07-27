from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """Abstract base class defining the common processing interface."""

    def __init__(self, name: str) -> None:
        self.name: str = name
        self._storage: list[tuple[int, str]] = []
        self._total: int = 0

    @property
    def total(self) -> int:
        """Total number of items ever ingested by this processor."""
        return self._total

    @property
    def remaining(self) -> int:
        """Number of items still stored, waiting to be extracted."""
        return len(self._storage)

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Check whether the data can be ingested by this processor."""

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Process the input data and store it internally."""

    def _store(self, items: list[str]) -> None:
        """Store items with their processing rank inside the processor."""
        for item in items:
            self._storage.append((self._total, item))
            self._total += 1

    def output(self) -> tuple[int, str]:
        """Extract and remove the oldest stored item with its rank."""
        if not self._storage:
            raise IndexError("No data left in this processor")
        return self._storage.pop(0)


class NumericProcessor(DataProcessor):
    """Processor for int, float and lists of both (mixed allowed)."""

    def __init__(self) -> None:
        super().__init__("Numeric Processor")

    @staticmethod
    def _is_number(data: Any) -> bool:
        return isinstance(data, (int, float)) and not isinstance(data, bool)

    def validate(self, data: Any) -> bool:
        if self._is_number(data):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(self._is_number(x) for x in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            self._store([str(x) for x in data])
        else:
            self._store([str(data)])


class TextProcessor(DataProcessor):
    """Processor for str and lists of str."""

    def __init__(self) -> None:
        super().__init__("Text Processor")

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            self._store(list(data))
        else:
            self._store([data])


class LogProcessor(DataProcessor):
    """Processor for dicts of string pairs and lists of such dicts."""

    def __init__(self) -> None:
        super().__init__("Log Processor")

    @staticmethod
    def _is_log(data: Any) -> bool:
        return (
            isinstance(data, dict)
            and len(data) > 0
            and all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in data.items()
            )
        )

    @staticmethod
    def _format(entry: dict[str, str]) -> str:
        return ": ".join(entry.values())

    def validate(self, data: Any) -> bool:
        if self._is_log(data):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(self._is_log(x) for x in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, dict):
            self._store([self._format(data)])
        else:
            self._store([self._format(x) for x in data])


class DataStream:
    """Routes stream elements to the appropriate registered processor."""

    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Register a new data processor for this stream."""
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        """Send each stream element to the first processor accepting it."""
        for element in stream:
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    break
            else:
                print(
                    "DataStream error - "
                    f"Can't process element in stream: {element}"
                )

    def print_processors_stats(self) -> None:
        """Print statistics about the registered processors."""
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(
                f"{proc.name}: total {proc.total} items processed, "
                f"remaining {proc.remaining} on processor"
            )


def main() -> None:
    """Test the polymorphic processing of a data stream."""
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    numeric = NumericProcessor()
    stream.register_processor(numeric)

    batch: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(text)
    stream.register_processor(log)
    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print(
        "\nConsume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        numeric.output()
    for _ in range(2):
        text.output()
    log.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
