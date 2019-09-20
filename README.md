# Simple example of reading data from mongo database and storing it into Postgres. 

## BEFORE MIGRATION
The Jira or Rally plugins should be loaded to the RP v5. If they are not found, nothing would migrate.
Specify RP_LAUNCH_KEEPFOR in milliseconds to migrate launches younger than. Specify -1 to store all launches.  


## Important information about migration
While importing projects and project_users relation, migration ignores non-existing users
assigned at the project, duplicate users, project without assigned users. Migration adds
"migrated_from" field in project metadata to mark that project was imported from MongoDB.

Projects are read from mongo by batch number and inserts one by one into postgres. Project users
and issue types are batched inserted into postgres per project. Attributes are batched inserted per project 
as well. Analyzer config properties "analyzer.minDocFreq = 1", "analyzer.minTermFreq = 1", "analyzer.minShouldMatch = 95", 
"analyzer.numberOfLogLines = 4". Email send rules are migrated per project.

Creator of bts doesn't exist in mongodb, so it is imported as login taken from basic auth or 'mongodb' if
doesn't exist.

## Recommendations before and after migration

To increase the speed of migration check the [official documentation](https://www.postgresql.org/docs/current/populate.html#POPULATE-COPY-FROM).
In short, it is a good idea to remove indexes and foreign keys and disable triggers before migration.
Return everything back after migration. Run VACUUM ANALYZE to update db planner.   
