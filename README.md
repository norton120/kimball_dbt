# Kimball\_dbt: dimensional modeling template using the DBT framework

---
[dbt docs](https://dbt.readme.io/docs)

---

## What is this? 
The DBT framework makes managing SQL transform code simple and sane. DBT abstracts much of the model dependency logic,
encouraging DRY development practices through the use of partials and macros. 

Kimball data warehousing methodology is widely reguarded as the defacto standard for data warehouse architecure and 
best practices. 

This is a template for using the power of DBT to implement a Kimball / Dimensional data warehouse that is driven
and maintained via the DBT framework! 


#### What is the basic flow of control?
Here it is in a nutshell:
![view on lucidchart](https://www.lucidchart.com/publicSegments/view/208a8045-b0d1-46ff-8213-e9ca1f515ddf/image.png)

- Source records are piped as-is into the data lake (referred to as the RAW database).
- Audits determine which records need to be quality checked each day.
- Screens are data quality tests that are applied to the source data. Resulting Error Events are logged.
- Staging tables contain the source data that has been audited for quality. 
- Slowly Changing Dimensions (SCDs) are applied, attributes are conformed, grains are algined, and production
  transforms applied to produce final production entities.


#### What are the bits
- QUALITY tables deal with source data quality.
- STAGING tables deal with middle-stage transforms.
- GUIDE tables contain meta about the Data Warehouse (data dictionary, conformed attributes etc). 
- GENERAL schema tables are used universally.
- MART schemas (Sales, Finance, Customer Service etc) are used to focus on business domains.


#### Why so flat? 
One of the challenges with heavily nested file structures in DBT is that they give a false sense of locality. All
model names are global - so a model at 

```
    /models/site_search/production/search_terms.sql
``` 

will conflict with

```
    /models/bing_browsing_data/partials/search_terms.sql
```

which is a nightmare when architecting your namespacing. To combat this, all the models materialized in the same 
namespace live in the same folder. This uses the filesystem to enforce unique model names - which is a hack, but 
a good one. 

This also means schemas need to be explicitly managed in the model file. Use the file template generator and you
will be fine. 
