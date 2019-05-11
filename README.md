This service implements a pipeline which attributes each r/borrow submission and comment to the user that it is relevant to.

# Attribution rules

This pipeline loops through the submissions in the core `submission` table, parsing each submission together with its child comments at each iteration. The parsed resource is assigned a type by the rules we provide below in the "SQL Table Dependencies" section.

Every resource parsed will produce at least one association, associating the resource to its source. We also attempt to identify any additional users referenced by the post and add additional associations for each reference found. The exception is that loan type resources are given an additional association with the user of the submission it was parsed under since the borrower is not typically explicitly mentioned in the comment indicating it.

Comments and posts made by the loans bot (`/u/LoansBot`, `t2_hz19i`) are excluded.


# SQL Table Dependencies

In addition to the `submissions` and `comments` tables documented in `collection_reddit_raw`, this service requires an additional tables for storing parsed data.

```
CREATE TABLE user_attribution (
  user_id VARCHAR(16),
  resource_id VARCHAR(16),
  PRIMARY KEY (user_id, resource_id),
  type VARCHAR,
  source BOOL
);
```

The type field takes one of the following values:

* `request`: A submission with `[req]` as part of its title string.
* `loan`: A comment containing `$loan`.
* `confirmation`: A comment containing `$confirm`.
* `unpaid`: A submission containing `[unpaid]` in the title string or a comment containing `$unpaid`.
* `paid`: A submission containing `[paid]` in the title string or a comment containing `$paid`.
* `other`: All other comments or submissions.

The source field, if true, indicates resource was made by the associated user (potentially about someone else). Otherwise, the resource was made by another user about the user associated.
