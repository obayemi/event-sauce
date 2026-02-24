- [ ] MINOR: Update macro test files to use new API (16 test instances need updating from `from_state` to `new()` or `from_snapshot()`)

- [ ] update the commands macro to also genreate a <command>\_event to allow easily constructing events for this aggregate

- [ ] make repositories generate from the main store, using its checkpointstore

- [ ] add a second example to showcase the implementation of a aggregate with the derive(Event) and ApplyEvent trait

- [ ] add the snapshot store as optional attribute to the event store to allow not requiring it when building subscription

- [ ] update examples to use projection! macro

- [ ] investigate if it would be possible to implement a version of the projection macro that would not require to write the event twice

- [ ] find how to reduce Aggregate boilerplate while not interfering with ability to not use the full framework. would it be usefull to implement an "entity" trait to allow non-event-sourced entity ?
- [ ] make apply_internal require events that implement ApplyEvent, drop manual match implementation
- [ ] make EventStore object safe by using pin<box<dyn Stream>> instead of "impl stream" to allow Arc<dyn EventStore>
- [ ] remove unsafe for specification
