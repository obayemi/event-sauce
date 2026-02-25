- [ ] make repositories generate from the main store, using its checkpointstore

- [ ] add the snapshot store as optional attribute to the event store to allow not requiring it when building subscription

- [ ] update examples to use projection! macro

- [ ] investigate if it would be possible to implement a version of the projection macro that would not require to write the event twice

- [ ] make EventStore object safe by using pin<box<dyn Stream>> instead of "impl stream" to allow Arc<dyn EventStore>
- [ ] remove unsafe for specification

- [ ] is there a best way to implement the "aggregate_type" method that is consistent across rust versions ?

- [ ] remove eventStoreRef
- [ ] unify test fixtures
- [ ] please implement a way to derive EventType from DomainEvent and make consistent use of the EventType type
- [ ] please remove the "try_into_event" from enveloppe
- [ ] make load / count_events free functions locals

- [ ] 6. ApplyEvent<A> vs EventApplicator<A> — not redundant but confusing naming
     These are complementary by design:
     - ApplyEvent<A> — implemented on individual event structs, defines validate() / apply() / post_validate()
     - EventApplicator<A> — implemented on event enums, dispatches to individual ApplyEvent impls via match
       The design is sound (visitor-like dispatch pattern), but the naming is confusing. "Apply" and "Applicator" read like synonyms. Names like
       EventHandler<A> (for the struct-level trait) or EventDispatcher<A> (for the enum-level trait) would be clearer.
       Verdict: Not redundant, but the naming creates confusion. Consider renaming EventApplicator to EventDispatcher to clarify the distinction.
       > rename possible to Event / EventDispatcher
