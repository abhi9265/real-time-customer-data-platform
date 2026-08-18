# Gold Product Analytics

## Metric grain

Every Gold table declares its grain:

| Model | Grain |
|---|---|
| `daily_customer_kpis` | one customer per event date |
| `daily_revenue` | one event date |
| `conversion_funnel` | one event date |
| `product_performance` | one product per event date |

Explicit grain prevents accidental fan-out joins and double counting.

## Funnel semantics

The canonical funnel is:

`product_viewed → cart_added → checkout_started → order_created`

Each stage counts distinct users, not raw events. This prevents a customer refreshing a product page ten times from being counted as ten funnel entrants.

## Revenue semantics

Revenue is sourced from `payment_completed` events and requires a valid non-negative payment amount. Event counts are not used as a revenue proxy.

## Incremental strategy

Gold processing should consume only affected event dates/products/customers from the current Silver micro-batch. Recomputing an affected partition is preferred to full-table scans when late-arriving events modify historical metrics.

## Serving contract

Gold models are designed for BI, SQL analytics, and feature generation. The semantic layer should expose stable metric names and definitions rather than embedding business logic in individual dashboards.
