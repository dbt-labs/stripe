# Stripe

A collection of SQL-based analytics for Stripe.

### Usage

All data models are built to be compiled and run with [dbt](https://github.com/analyst-collective/dbt). Installation:

1. Add this package as a dependency to your project and run `dbt deps` to download the latest source.
1. Add the following configuration to your `dbt_project.yml`:

```YAML
#don't duplicate this
models:                                     
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
