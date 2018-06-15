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



#### Why so flat? 
One of the challenges with heavily nested file structures in DBT is that they give a false sense of locality. All
model names are global - so a model at 

```
    /models/site\_search/production/search\_terms.sql
``` 

will conflict with

```
    /models/bing\_browsing\_data/partials/search\_terms.sql
```

which is a nightmare when architecting your namespacing. To combat this, all the models materialized in the same 
namespace live in the same folder. This uses the filesystem to enforce unique model names - which is a hack, but 
a good one. 

This also means schemas need to be explicitly managed in the model file. Use the file template generator and you
will be fine. 
