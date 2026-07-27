*This project has been created as part of the 42 curriculum by aalkhati.*

# Python Module 05 — Code Nexus (OOP)

Data processing architecture built with Python object-oriented
programming: abstract base classes, method overriding, polymorphism,
and static duck typing with Protocol.

## Project structure

```
py05/
├── README.md
├── ex0/
│   └── data_processor.py   # Abstract DataProcessor + 3 specialized processors
├── ex1/
│   └── data_stream.py      # DataStream: polymorphic routing + statistics
└── ex2/
    └── data_pipeline.py    # Export plugins (CSV / JSON) via Protocol
```

## How to run

Each exercise is a standalone script:

```bash
python3 ex0/data_processor.py
python3 ex1/data_stream.py
python3 ex2/data_pipeline.py
```

Requirements: Python 3.10+ (uses `int | float` union syntax).
Only `abc` and `typing` are imported.

## The exercises

### ex0 — Data Processor (abstract foundation)

`DataProcessor(ABC)` is the contract every processor must honor: two
abstract methods (`validate`, `ingest`) that subclasses are forced to
implement, plus the shared machinery written once in the base class —
a FIFO storage of `(rank, str)` tuples, a lifetime counter that
doubles as the processing rank, and the standard `output()` method
that pops the oldest item. Three concrete processors specialize it:

- **NumericProcessor** accepts `int`/`float` or non-empty lists of
  them (booleans deliberately rejected) and stores values as strings.
- **TextProcessor** accepts `str` or non-empty lists of `str`.
- **LogProcessor** accepts dicts whose keys AND values are all
  strings (or lists of such dicts), formatted by joining the values
  with `": "` — e.g. `"NOTICE: Connection to server"`.

Every `ingest` re-validates defensively and raises
`ValueError("Improper ... data")` on bad input. The test scenario
demonstrates validation answers, the guarded exception, ingestion of
a batch, and rank-tagged extraction.

### ex1 — Data Stream (polymorphism in action)

`DataStream` holds a `list[DataProcessor]` — typed against the
abstract class, never a concrete one. `register_processor()` adds a
processor; `process_stream()` walks a mixed stream and hands each
element to the FIRST processor whose `validate()` says yes, using a
`for...else` so unclaimed elements print a
`DataStream error - Can't process element in stream` message.
`print_processors_stats()` reports, for every processor, the lifetime
total ingested and the items still waiting — exposed through
read-only `@property` accessors (`total`, `remaining`). The scenario
shows the same batch failing partially with one processor registered,
then fully succeeding once all three are; consuming items lowers
`remaining` while `total` keeps its history.

### ex2 — Data Pipeline (Protocol / duck typing)

`ExportPlugin(Protocol)` declares a structural interface: anything
with `process_output(data: list[tuple[int, str]]) -> None` qualifies.
`CSVExportPlugin` (values joined by commas) and `JSONExportPlugin`
(`"item_<rank>": "value"` pairs, hand-escaped since `json` is not
imported) inherit from NOTHING — mypy accepts them purely by shape.
`DataStream.output_pipeline(nb, plugin)` drains up to `nb` items per
processor — capped with `min(nb, remaining)` so over-asking never
raises — and hands each non-empty batch to the plugin. Ranks assigned
at ingestion survive into the export, which is why the JSON output
shows `item_3`, `item_4`, ... even after earlier items were consumed.

## Code quality

```bash
flake8 ex0/ ex1/ ex2/     # passes clean
mypy ex0/ ex1/ ex2/       # clean, except ONE intentional error in ex0
```

The single mypy error in ex0 (`ingest("foo")` called with a string)
is required by the subject and demonstrates the two layers of
protection working together:

1. **Static layer** — `ingest` is annotated
   `int | float | list[int | float]`; passing a string violates that
   contract, so mypy flags the call before the program even runs.
2. **Runtime layer** — type hints are not enforced during execution,
   so `ingest` re-validates and raises
   `ValueError("Improper numeric data")`, which the test catches and
   prints.

One line, two safety nets. Do not "fix" this error: removing it would
make the project non-compliant with the subject.

## Concepts covered

- **Abstract Base Classes (`abc`)**: `DataProcessor(ABC)` cannot be
  instantiated; `@abstractmethod` forces every subclass to implement
  `validate()` and `ingest()`. Shared logic (`_store`, `output`, rank
  counter) lives once in the base class.
- **Method overriding**: each processor overrides `validate(data: Any)`
  with the same wide signature, but narrows `ingest()` to the types it
  accepts (numbers, strings, or string-pair dicts — and lists of each).
- **Polymorphism**: `DataStream` holds a `list[DataProcessor]` and calls
  `validate`/`ingest` without knowing concrete types; Python dispatches
  to the right override at runtime. New processor types can be added
  without changing a single line of `DataStream` (open/closed principle).
- **Protocol / duck typing**: `ExportPlugin(Protocol)` defines a
  structural interface. `CSVExportPlugin` and `JSONExportPlugin` inherit
  from nothing — they qualify because they have the right
  `process_output()` shape, checked statically by mypy.
- **FIFO storage with ranks**: items are stored as `(rank, str)` tuples;
  ranks are assigned at ingestion and never reset, so exports show the
  lifetime processing order (`item_3`, `item_4`, ...).

## Design choices worth noting

- `bool` is excluded from numeric validation because `bool` is a
  subclass of `int` in Python (`True` would otherwise validate as `1`).
- Empty lists fail validation in every processor: `all([])` is `True`
  (vacuous truth), so without a length guard `[]` would be claimed by
  whichever processor registered first.
- `output_pipeline` pops `min(nb, remaining)` items per processor so
  asking for more than available never raises.
