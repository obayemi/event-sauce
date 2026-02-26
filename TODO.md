- [ ] make repositories generate from the main store, using its checkpointstore

- [ ] add the snapshot store as optional attribute to the event store to allow not requiring it when building subscription

- [ ] update examples to use projection! macro

- [ ] investigate if it would be possible to implement a version of the projection macro that would not require to write the event twice

- [ ] make EventStore object safe by using pin<box<dyn Stream>> instead of "impl stream" to allow Arc<dyn EventStore>
- [ ] remove unsafe for specification

- [ ] is there a best way to implement the "aggregate_type" method that is consistent across rust versions ?

- [ ] update the postgresbackend to store arcs instead of actual values to allow easy sharing without requireing creating new arcs
