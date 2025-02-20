# Simple DB

## Table

Table has a schema. Every table has a primary key which should be a string (VARCHAR) of any size.

| id      | counter |
|---------|---------|
| Alice   | 0       |
| Bob     | 12      |
| Charlie | 13      |

To create such a table, run:

```
CREATE TABLE table_name (
    id VARCHAR(12) PRIMARY KEY,
    counter INTEGER;
);
```

To select all columns for Alice, run:

```
SELECT * FROM table_name
WHERE id = 'Alice';
```

To insert a row, run:

```
INSERT INTO table_name (id, counter)
VALUES (Paul, 0);

```