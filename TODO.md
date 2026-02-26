- [ ] remove unsafe or code that panics everywhere

- [ ] add the snapshot store as optional attribute to the event store to allow not requiring it when building subscription

- [ ] investigate if it would be possible to implement a version of the projection macro that would not require to write the event twice

- [ ] make EventStore object safe by using pin<box<dyn Stream>> instead of "impl stream" to allow Arc<dyn EventStore>

- [ ] is there a best way to implement the "aggregate_type" method that is consistent across rust versions ?

- [x] update the postgresbackend to store arcs instead of actual values to allow easy sharing without requireing creating new arcs

- [x] Aggregate / uninitialized aggregate system to have one or many "initialization" events, and allow stricter Entity design without needing to accomodate uninitialized states at aggregate creation. add "init" flag to those events in the macro to allow them to take an emptyaggregate and  return a full aggregate
  - [x] base implementation
  - [x] fix projections

- [x] design an api to add @init commands that will be defined on the repository
  - [x] init creation functions: `Aggregate::cmd()` and `Aggregate::cmd_with_id()`
  - [x] remove @with_init — unified entry points for command_handler! and define_events!
  - [x] update postgres-quickstart to use commands everywhere
