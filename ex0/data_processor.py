from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """Abstract base class defining the common processing interface."""

    def __init__(self, name: str) -> None:
        self.name: str = name
        self._storage: list[tuple[int, str]] = []
        self._total: int = 0

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


def main() -> None:
    """Test the data processor architecture."""
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    print(f"Trying to validate input '42': {numeric.validate(42)}")
    print(f"Trying to validate input 'Hello': {numeric.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest("foo")
    except ValueError as exc:
        print(f"Got exception: {exc}")
    numbers: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {numbers}")
    numeric.ingest(numbers)
    print("Extracting 3 values...")
    for _ in range(3):
        rank, value = numeric.output()
        print(f"Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f"Trying to validate input '42': {text.validate(42)}")
    words = ["Hello", "Nexus", "World"]
    print(f"Processing data: {words}")
    text.ingest(words)
    print("Extracting 1 value...")
    rank, value = text.output()
    print(f"Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")
    logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f"Processing data: {logs}")
    log.ingest(logs)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, value = log.output()
        print(f"Log entry {rank}: {value}")


if __name__ == "__main__":
    main()
