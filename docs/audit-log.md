# Audit Log Guide

This guide explains the built-in audit log and traceability features in event-sauce.

## Table of Contents

- [Overview](#overview)
- [What Gets Tracked](#what-gets-tracked)
  - [Actor Tracking (created_by)](#actor-tracking-created_by)
  - [Causation Tracking](#causation-tracking)
  - [Custom Metadata](#custom-metadata)
- [Querying the Event Log](#querying-the-event-log)
  - [EventLogQuery Trait](#eventlogquery-trait)
  - [Filtering](#filtering)
  - [Pagination](#pagination)
  - [Discovering Types](#discovering-types)
- [Causation Chains](#causation-chains)
- [Best Practices](#best-practices)

## Overview

Every event stored by event-sauce automatically captures audit information: who performed the action, when it happened, and what caused it. The `EventLogQuery` trait provides paginated, filtered access to this history for compliance audits, admin dashboards, and debugging.

**Built-in traceability fields on every `EventEnvelope`:**

| Field | Type | Source |
|-------|------|--------|
| `id` | `Uuid` | Auto-generated unique event ID |
| `aggregate_id` | `Uuid` | Which aggregate instance |
| `aggregate_type` | `AggregateType` | Which aggregate kind (e.g., "User") |
| `event_type` | `String` | What happened (e.g., "User.Registered") |
| `created_by` | `Option<Uuid>` | Who triggered the event (actor ID) |
| `created_at` | `DateTime<Utc>` | When the event was created |
| `metadata` | `Option<EventMetadata>` | Causation chain, correlation ID, custom data |

## What Gets Tracked

### Actor Tracking (created_by)

When using [actor events](events.md), the actor's entity ID is automatically stored in `EventEnvelope::created_by`:

```rust
define_events! {
    pub enum OrderEvent for Order {
        Placed {
            items: Vec<String>,
        }
        @actor(User)
        @validate |_order, actor, _evt| {
            if !actor.can_place_orders {
                return Err(OrderError::Forbidden);
            }
            Ok(())
        }
        => |order, event| {
            order.items = event.items.clone();
        },
    }
}

// When committed, the event envelope will have:
// created_by: Some(user.entity_id())
```

You can also set `created_by` manually using `apply_with_actor()`:

```rust
let mut order = AggregateRoot::<Order>::new(EntityId::new());
order.apply_with_actor(some_event, user_id)?;
// The event's created_by will be set to user_id
```

### Causation Tracking

When [policies](policies.md) react to events, causation metadata is automatically injected into the resulting events:

```rust
// Source event: UserKickedEvent (id = "aaa")
//   → Policy produces: GroupMemberRemovedEvent
//       causation_id: "aaa"           (direct cause)
//       correlation_id: "aaa"         (root cause)
//       causation_chain: ["aaa"]      (full ancestry)
//
//   → That triggers another policy: NotificationSentEvent
//       causation_id: "bbb"           (direct cause = GroupMemberRemovedEvent)
//       correlation_id: "aaa"         (root cause = original UserKickedEvent)
//       causation_chain: ["aaa", "bbb"]  (full ancestry)
```

This is fully automatic — `PolicyContext::commit()` builds the causation chain from the source event.

### Custom Metadata

Attach application-specific data using `EventMetadata::additional`:

```rust
use event_sauce_core::EventMetadata;
use serde_json::json;

let metadata = EventMetadata::new()
    .with_additional(json!({
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0",
        "tenant_id": "acme-corp",
    }));

// Apply event with metadata
aggregate.apply_with_metadata(some_event, metadata)?;
```

The `additional` field is stored as JSONB in PostgreSQL, so it's queryable and flexible.

## Querying the Event Log

### EventLogQuery Trait

The `EventLogQuery` trait provides read-only access to the event store:

```rust
use event_sauce_core::{EventLogQuery, EventLogParams};

async fn show_recent_events(query: &dyn EventLogQuery) -> Result<(), Box<dyn std::error::Error>> {
    let params = EventLogParams::default(); // page 0, 50 per page
    let page = query.query_events(params).await?;

    println!("Total events: {}", page.total_count);
    println!("Pages: {}", page.total_pages());

    for entry in &page.entries {
        println!(
            "[{}] {} on {} (by {:?})",
            entry.position.as_i64(),
            entry.envelope.event_type,
            entry.envelope.aggregate_type,
            entry.envelope.created_by,
        );
    }

    Ok(())
}
```

### Filtering

`EventLogParams` supports multiple filters combined with AND logic:

```rust
use event_sauce_core::EventLogParams;

// Find all events by a specific user
let params = EventLogParams {
    created_by: Some(user_uuid),
    ..Default::default()
};

// Find all Order events in a time range
let params = EventLogParams {
    aggregate_type: Some("Order".to_string()),
    from_date: Some(start_of_day),
    to_date: Some(end_of_day),
    ..Default::default()
};

// Find a specific event type for a specific aggregate
let params = EventLogParams {
    event_type: Some("User.EmailChanged".to_string()),
    aggregate_id: Some(user_uuid),
    per_page: 10,
    ..Default::default()
};
```

Available filters:

| Filter | Type | Description |
|--------|------|-------------|
| `aggregate_type` | `Option<String>` | Filter by aggregate kind (e.g., "User") |
| `event_type` | `Option<String>` | Filter by event type name |
| `aggregate_id` | `Option<Uuid>` | Filter by specific aggregate instance |
| `created_by` | `Option<Uuid>` | Filter by actor who created the event |
| `from_date` | `Option<DateTime<Utc>>` | Events created at or after |
| `to_date` | `Option<DateTime<Utc>>` | Events created at or before |

### Pagination

Results are paginated with zero-indexed pages:

```rust
let params = EventLogParams {
    page: 0,       // First page (default)
    per_page: 25,  // 25 entries per page (default: 50)
    ..Default::default()
};

let page = query.query_events(params).await?;

// Navigate pages
println!("Page {} of {}", page.page + 1, page.total_pages());
println!("Showing {} of {} total events", page.entries.len(), page.total_count);
```

Results are ordered by position descending (newest first).

### Discovering Types

Use `distinct_aggregate_types()` and `distinct_event_types()` to discover what's in the store — useful for building filter dropdowns in admin UIs:

```rust
let aggregate_types = query.distinct_aggregate_types().await?;
// e.g., ["User", "Order", "Payment"]

let event_types = query.distinct_event_types().await?;
// e.g., ["User.Registered", "User.EmailChanged", "Order.Placed", ...]
```

## Causation Chains

Causation chains let you trace exactly why an event happened, all the way back to the original user action:

```
User clicks "Kick Member" button
  └─ UserKickedEvent (id=aaa, correlation_id=aaa)
       └─ [KickPolicy] GroupMemberRemovedEvent (causation_id=aaa, correlation_id=aaa, chain=[aaa])
            └─ [NotifyPolicy] NotificationSentEvent (causation_id=bbb, correlation_id=aaa, chain=[aaa, bbb])
```

**Fields:**

- **`correlation_id`** — the root event that started the entire chain (stays the same throughout)
- **`causation_id`** — the immediate parent event (changes at each step)
- **`causation_chain`** — full ancestry from root to direct parent, ordered chronologically

These fields are populated automatically by `PolicyContext`. Direct user commands have no causation metadata (they *are* the root cause).

## Best Practices

1. **Use actor events** for commands that should be attributed to a user — the `@actor(Type)` annotation handles `created_by` automatically
2. **Use policies** for cross-aggregate reactions — causation metadata is injected automatically
3. **Store context in `additional`** — IP addresses, tenant IDs, and session info belong in `EventMetadata::additional`, not in event data
4. **Query by correlation_id** to find all events in a causal chain — this is the most useful debugging tool for understanding cascading effects
5. **Set reasonable page sizes** — default is 50, but adjust for your UI needs

Next: [Claims Guide](claims.md) | [Policies Guide](policies.md)
