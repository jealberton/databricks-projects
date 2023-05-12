# databricks-projects
Some projects and analysis made in Databricks environment with spark sql and pyspark

- % Orders with Items Observation - Context: In a groceries dlivery app, it was being considered to disable a function that allowed the user to make short observations on any item being purchased (for example: "Please I want  this banana to be very ripe", or "please remember that the cheese must be lactose-free"). The goal was cost reduction, with the argument that it was not being widely used, but to make that decision it was necessary to know the size of this impact. This query surveys the % of orders that had at least 1 item observed, considering a fixed period of time and a selected group of stores

- Categories_correlations - context: Wanted to know which product categories most influenced the picking time of a supermarket order

- Fetch Last Status - Context: From a stamp table which contained the store id, the status of the day and the date, it was desired to create a new dataset containing only the store's current status and from when this status started. The query contains logic using window functions to compare one line after another and set what the current status is and then fetch the date

- Remodel_ds_with_window_function - Context: From a dataset containing only the columns: Status, OldValue, NewValue and Date, build a new dataset with more friendly formatting, containing: Status name, status-start, status-end
