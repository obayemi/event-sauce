- [ ] MINOR: Update macro test files to use new API (16 test instances need updating from `from_state` to `new()` or `from_snapshot()`)

- [ ] update the commands macro to also genreate a <command>\_event to allow easily constructing events for this aggregate

- [ ] make repositories generate from the main store, using its checkpointstore

- [ ] add a second example to showcase the implementation of a aggregate with the derive(Event) and ApplyEvent trait

- [ ] add the snapshot store as optional attribute to the event store to allow not requiring it when building subscription

- [ ] update examples to use projection! macro

- [ ] investigate if it would be possible to implement a version of the projection macro that would not require to write the event twice
