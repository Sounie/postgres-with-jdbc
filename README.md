Dabbling with Postgres from Java.

The code demonstrates how PostgreSQL could be made to behave similar to ElasticSearch when updates only apply if the specified version is greater than the existing record version.

In a situation where updates are more common than inserts, it is tempting to come up with a mechanism whereby an update is attempted before falling back to an insert when the record does not already exist, but the versioning clause would mean that updates could come back as changing no rows even when a record with the specific id exists.

