from abc import ABC, abstractmethod
from typing import Any, Protocol


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


class ExportPlugin(Protocol):
    """Structural interface every export plugin must satisfy."""

    def process_output(self, data: list[tuple[int, str]]) -> None:
        """Export a batch of processed data."""
        ...


class CSVExportPlugin:
    """Exports processed data as a CSV line (duck-typed plugin)."""

    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(value for _, value in data))


class JSONExportPlugin:
    """Exports processed data as a JSON object (duck-typed plugin)."""

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        entries = ", ".join(
            f'"item_{rank}": "{self._escape(value)}"'
            for rank, value in data
        )
        print("{" + entries + "}")


class DataStream:
    """Routes stream elements to processors and exports their output."""

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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        """Consume up to nb elements per processor and export them."""
        for proc in self._processors:
            batch: list[tuple[int, str]] = []
            for _ in range(min(nb, proc.remaining)):
                batch.append(proc.output())
            if batch:
                plugin.process_output(batch)


def main() -> None:
    """Test the complete data pipeline."""
    print("=== Code Nexus - Data Pipeline ===\n")

    print("Initialize Data Stream...\n")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

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
    print(f"\nSend first batch of data on stream: {batch}\n")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSVExportPlugin())
    print()
    stream.print_processors_stats()

    batch2: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash",
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"\nSend another batch of data: {batch2}\n")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSONExportPlugin())
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
