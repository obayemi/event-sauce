- [ ] MINOR: Update macro test files to use new API (16 test instances need updating from `from_state` to `new()` or `from_snapshot()`)

- [ ] update the commands macro to also genreate a <command>\_event to allow easily constructing events for this aggregate

- [ ] please reorganise the examples to include a single "postgres-quickstart" showcasing all the easy macro niceties for with two aggregats working together and all their events and some postgres backed event store and projections, then add Aggregates, then do add specific examples showcasing the different levels of manual implementation of the different features in the library (like manual event enum with ApplyEvent, or manual aggregate commands)

- [ ] make repositories generate from the main store, using its checkpointstore
