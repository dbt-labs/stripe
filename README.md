# Stripe

A collection of SQL-based analytics for Stripe.

Please note that, while the `core_entities` and `event_filters` models are very generic and likely relevant to every business that uses stripe, the `transactions_prep` and `mrr` models may or may not be applicable to your business. If you find them useful in your analytics, great! If not, disable them in your project by setting `enabled: false` for the relevant folders within your `dbt_project.yml`.

### Usage

All data models are built to be compiled and run with [dbt](https://github.com/analyst-collective/dbt). Installation:

1. Add this package as a dependency to your project and run `dbt deps` to download the latest source. We recommend that you reference a specific tag so that you can control the upgrade process when new versions are released.
1. Add the following configuration to your `dbt_project.yml`:

```YAML
# within `models:`
  stripe:
    enabled: true
    materialized: view
    vars:
      #insert the location of your stripe_events table here as 'schema.table'
      events_table: 'stripe.stripe_events'  
```

### Contributing
Contributions are welcome! To contribute:
- fork this repo,
- make and test changes, and
- submit a PR.

All contributions must be widely relevant to Stripe customers and not contain logic specific to a given business.
